--- Временный узел: настоящий `box` в дочернем процессе на время проверки.
---
--- В процессе проверок `box` не настроен, и настраивать его там нельзя:
--- один процесс — один `box.cfg`, а проверок сотни. Поэтому всё, что
--- проверяется против настоящего ядра — индекс, транзакция, применение
--- конфигурации, — идёт на узле, поднятом `luatest.Server` в своём
--- временном каталоге. Узел — тоже процесс Tarantool, и загрузчик `.rocks`
--- в нём тот же: исходники пакета подставляются в его `package.loaded`
--- абсолютными путями, иначе проверка на узле шла бы против установленной
--- копии.

local fio = require('fio')
local socket = require('socket')
local t = require('luatest')

local files = require('tnt.testing.files')
local sources = require('tnt.testing.sources')

local Module = {}

--- Сколько памяти даётся узлу, если не сказано иного: проверкам данные
--- нужны небольшие, а арена выделяется целиком при подъёме.
Module.DEFAULT_MEMORY = 64 * 1024 * 1024

---@class TntTestingNodeOptions
---@field alias string|nil Имя узла в артефактах прогона; по умолчанию node
---@field modules TntTestingSource[]|nil Исходники, которые узел берёт вместо установленных копий
---@field box_cfg table|nil Настройки `box.cfg` поверх умолчаний
---@field net_box_port integer|nil Порт iproto на TCP; по умолчанию узел слушает unix-сокет

--- Поднимает одиночный узел с исходниками пакета.
---
--- Это единственная дверь к узлу с голым `box_cfg`: прямой
--- `t.Server:new` без своего сценария оставил бы узел без строгого
--- режима глобалов. Порт TCP нужен тому, кто ходит к узлу не через
--- net.box, — дымовой проверке, которая стучится в iproto как в HTTP.
---
--- Возвращает сервер, который проверка обязана остановить сама — `stop`.
---@param opts TntTestingNodeOptions|nil
---@return table server
function Module.start(opts)
    local given = opts or {}
    local cfg = { memtx_memory = Module.DEFAULT_MEMORY }

    for key, value in pairs(given.box_cfg or {}) do
        cfg[key] = value
    end

    local server = t.Server:new({
        alias = given.alias or 'node',
        workdir = fio.tempdir(),
        net_box_port = given.net_box_port,
        box_cfg = cfg,
    })

    server:start()

    -- Строгий режим глобалов — как в процессе проверок и на узле
    -- `configured`: код ролей, миграций и обработчиков, который проверка
    -- гоняет на узле, тот же, и опечатка в глобале должна падать и здесь.
    -- Сценарий узла — чужой, из luatest, поэтому режим включается первым
    -- запросом, до исходников: опечатка в функции, которую исходник
    -- зовёт при загрузке, тоже падает. `server:restart` снимает режим
    -- вместе с исходниками: узел, нужный заново, поднимают снова `start`.
    server:exec(function(modules)
        require('strict').on()

        for _, module in ipairs(modules) do
            package.loaded[module.name] = assert(loadfile(module.path))()
        end
    end, { sources.absolute(given.modules or {}) })

    return server
end

--- Останавливает узел и убирает его каталог.
---
--- Каталог у узла по конфигурации — тот, куда он перешёл (`chdir`);
--- у узла с голым `box_cfg` — рабочий. Журнал узла luatest держит
--- у себя, и он остаётся в артефактах прогона.
---@param server table
function Module.stop(server)
    local directory = server.chdir or server.workdir

    server:drop()
    fio.rmtree(directory)
end

--- Поднимает одиночный узел по декларативной конфигурации.
---
--- Нужен там, где проверяется договор с самим ядром — с применением ролей,
--- доской предупреждений, перечитыванием, — а узел с голым `box_cfg`
--- этого не показывает. Каталог свой: проверка обязана остановить узел
--- `stop`, и каталог уйдёт вместе с ним.
---
--- Сценарий у узла есть, хотя он пустой, и конфигурация приходит
--- окружением (`TT_CONFIG`), а не режимом luatest `config_file`. Узел
--- без сценария читает Lua из унаследованного stdin, и там, где труба
--- не закрыта, замирает целиком: подъём проходит, а на запросы узел
--- не отвечает, и luatest ждёт готовности до срока.
---
--- Модули перечисленных пакетов берутся из исходников. Корень поиска
--- luatest ставит на дерево проекта, и его загрузчик `.rocks` нашёл бы
--- поставленную копию раньше — проверялся бы вчерашний код.
---@param name string Имя инстанса
---@param instance string[] Поля инстанса в строчной записи YAML, по одному на элемент
---@param packages string[] Каталоги пакетов в `libs/`, например `tnt-health`
---@return table server
function Module.configured(name, instance, packages)
    local workdir = fio.tempdir()
    local lines = {
        'credentials: {users: {guest: {roles: [super]}}}',
        'groups: {g: {replicasets: {r: {instances: {' .. name .. ': {',
        "  iproto: {listen: [{uri: 'unix/:./instance.iproto'}]},",
    }

    for _, line in ipairs(instance) do
        table.insert(lines, '  ' .. line .. ',')
    end

    table.insert(lines, '}}}}}}')

    files.write(fio.pathjoin(workdir, 'config.yaml'), table.concat(lines, '\n'))

    -- Сценарий зовётся после применения конфигурации: отметка готовности
    -- в нём и есть «узел поднят», которую ждёт luatest. Строгий режим
    -- глобалов включается и на поднятом узле: роли и обработчики, которые
    -- проверка гоняет на нём, — тот же код, что в процессе проверок, и
    -- опечатка в глобале там должна падать так же, а не проходить молча.
    files.write(fio.pathjoin(workdir, 'init.lua'), "require('strict').on()\n_G.ready = true\n")

    local root = fio.cwd()
    local rocks = fio.pathjoin(root, '.rocks')
    local paths = {}

    for _, package in ipairs(packages) do
        table.insert(paths, fio.pathjoin(root, 'libs', package, '?.lua'))
    end

    table.insert(paths, fio.pathjoin(rocks, 'share', 'tarantool', '?.lua'))
    table.insert(paths, fio.pathjoin(rocks, 'share', 'tarantool', '?', 'init.lua'))

    -- Сервер HTTP держит разбор запроса в собранной библиотеке.
    -- Оба расширения: luarocks кладёт `.so`, а сборка cmake
    -- на macOS — `.dylib` (рок watchdog).
    local libraries = {
        fio.pathjoin(rocks, 'lib', 'tarantool', '?.so'),
        fio.pathjoin(rocks, 'lib', 'tarantool', '?.dylib'),
    }

    local server = t.Server:new({
        alias = name,
        command = arg[-1],
        args = { 'init.lua' },
        chdir = workdir,
        net_box_uri = 'unix/:' .. fio.pathjoin(workdir, 'instance.iproto'),
        setsearchroot = false,
        env = {
            TT_CONFIG = 'config.yaml',
            TT_INSTANCE_NAME = name,
            LUA_PATH = table.concat(paths, ';') .. ';',
            LUA_CPATH = table.concat(libraries, ';') .. ';',
        },
    })

    server:start({ wait_until_ready = true })

    return server
end

--- Свободный порт TCP: сокет спрашивает его у системы и тут же отпускает.
---
--- Роль HTTP нулевого порта не принимает, а заранее выбранный порт,
--- занятый соседом, уронил бы подъём узла, а не проверку. Между
--- отпусканием и подъёмом порт может занять сосед — окно короткое,
--- и живые проверки с ним живут.
---@return integer
function Module.free_port()
    ---@type any
    local probe = socket.tcp_server('127.0.0.1', 0, function() end)
    local port = probe:name().port

    probe:close()

    return math.floor(port)
end

return Module
