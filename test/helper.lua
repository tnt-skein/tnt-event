--- Общие средства проверок шины событий.
---
--- Шина живёт в памяти, и проверять её можно прямо в процессе проверок:
--- настоящие файберы, настоящие каналы, настоящий контекст. К `box` шина
--- обращается только за транзакцией вызывающего, и это её внешняя
--- зависимость — ветка «в транзакции» идёт и двойником (порядок и записи),
--- и на настоящем узле (`txn_node_test.lua`).
---
--- Исходники читаются с диска, а не через `require`: у Tarantool свой
--- загрузчик `.rocks`, он идёт раньше `package.path` и подсунул бы
--- установленную копию пакета, если она есть. Проверки тогда шли бы
--- против вчерашнего кода, а покрытие считалось бы по нему. Зависимости
--- пакета — `tnt.message`, `tnt.must`, `tnt.clock`, `tnt.context`,
--- `tnt.log`, `tnt.storage`, `tnt.external` — берутся из `.rocks` обычным
--- `require`: проверяется этот пакет, а не они. На временном узле они
--- берутся так же.
---
--- Оснастка в `test/testing/` — загрузчик исходников, ловушка журнала,
--- запись файлов и временный узел — грузится так же, файлами, и один раз
--- на процесс: второй экземпляр загрузчика не знал бы, что вытеснил первый,
--- и не вернул бы вытесненное на место.
---
--- Проверки берут всё через этот помощник, а не из оснастки напрямую:
--- помощник — единственное, чем файл проверок отличается от того же файла
--- в наборе, где пакет живёт рядом со своими зависимостями.

local fio = require('fio')
local t = require('luatest')

--- Реестр встроенного metrics: его методов в аннотациях ядра нет.
---@type any
local registry = require('metrics')

--- Модули оснастки в порядке зависимостей: узел берёт файлы и загрузчик,
--- ловушка журнала — загрузчик.
local TESTING = {
    { name = 'tnt.testing.sources', path = 'test/testing/sources.lua' },
    { name = 'tnt.testing.files', path = 'test/testing/files.lua' },
    { name = 'tnt.testing.journal', path = 'test/testing/journal.lua' },
    { name = 'tnt.testing.node', path = 'test/testing/node.lua' },
}

for _, module in ipairs(TESTING) do
    if package.loaded[module.name] == nil then
        local chunk, failure = loadfile(fio.abspath(module.path))

        if chunk == nil then
            error(('оснастка %s не читается: %s'):format(module.name, tostring(failure)))
        end

        package.loaded[module.name] = chunk()
    end
end

--- Оснастка проверок под теми именами, что зовёт помощник.
local testing = {
    load_sources = package.loaded['tnt.testing.sources'].load,
    module = package.loaded['tnt.testing.sources'].module,
    capture_log = package.loaded['tnt.testing.journal'].capture,
    start_node = package.loaded['tnt.testing.node'].start,
    stop_node = package.loaded['tnt.testing.node'].stop,
}

--- Общий способ объявить ряд и ряды договора — файлами из `.rocks`, свой
--- экземпляр на каждую загрузку помощника, а не `require`.
---
--- Ряды — состояние процесса: повторное объявление отдаёт прежний ряд
--- и ставит шкале глубины источник последней загрузки. Пакет ставит свой
--- источник глубины при загрузке, и с одним экземпляром на процесс глубину
--- считал бы пакет последнего загруженного файла проверок, а не тот, что
--- под проверкой. Со своим экземпляром ряды каждой загрузки возвращаются
--- в реестр первым же наблюдением, и проверка видит свою глубину.
local ROWS = {
    { name = 'tnt.metrics.series.labels', path = '.rocks/share/tarantool/tnt/metrics/series/labels.lua' },
    { name = 'tnt.metrics.series.collector', path = '.rocks/share/tarantool/tnt/metrics/series/collector.lua' },
    { name = 'tnt.metrics.series', path = '.rocks/share/tarantool/tnt/metrics/series.lua' },
    { name = 'tnt.message.series', path = '.rocks/share/tarantool/tnt/message/series.lua' },
}

--- Модули в порядке зависимостей: ряды, затем пакет.
local OWN = {
    ROWS[1],
    ROWS[2],
    ROWS[3],
    ROWS[4],
    { name = 'tnt.event.listener', path = 'tnt/event/listener.lua' },
    { name = 'tnt.event', path = 'tnt/event.lua' },
}

--- Средства проверок шины.
---@class TntEventTestHelper
---@field event table Фасад шины
---@field listener table Слушатель шины
---@field context table Контекст файбера
---@field series table Ряды метрик договора
local helper = { MODULES = OWN }

--- Фасад пакета из исходников: в процессе проверок грузится один раз,
--- а состояние шины проверки возвращают сами (`forget`).
helper.event = testing.load_sources(helper.MODULES, 'tnt.event')

-- Части пакета берутся из той же загрузки, что и фасад: взятые `require`,
-- они пришли бы установленной копией из `.rocks`.
helper.listener = testing.module('tnt.event.listener')

helper.context = require('tnt.context')

--- Ряды договора — из той же загрузки, что и шина: её источник глубины
--- стоит в них.
helper.series = testing.module('tnt.message.series')

--- Совпадают ли метки: одинаковый набор имён с одинаковыми значениями.
---@param left table
---@param right table
---@return boolean
local function same_labels(left, right)
    for name, value in pairs(left) do
        if right[name] ~= value then
            return false
        end
    end

    for name in pairs(right) do
        if left[name] == nil then
            return false
        end
    end

    return true
end

--- Наблюдения реестра по имени ряда — после источников шкал, как их
--- собирает выкладка.
---@param name string
---@return table[]
function helper.samples(name)
    local found = {}

    for _, observation in ipairs(registry.collect({ invoke_callbacks = true })) do
        if observation.metric_name == name then
            table.insert(found, observation)
        end
    end

    return found
end

--- Что увидит сборщик: число ряда с ровно такими метками.
---@param name string
---@param labels table|nil
---@return number|nil
function helper.value(name, labels)
    for _, observation in ipairs(helper.samples(name)) do
        if same_labels(observation.label_pairs, labels or {}) then
            return observation.value
        end
    end

    return nil
end

--- Ловушка журнала: записи о зарытом, сорвавшемся крюке и пропавшем
--- событии видны только ею.
helper.journal = testing.capture_log()

--- Поднимает временный узел с исходниками пакета: настоящие
--- `box.is_in_txn` и `box.on_commit`. Узел проверка обязана остановить
--- сама — `stop_node`.
---@return table server
function helper.start_node()
    return testing.start_node({ modules = helper.MODULES })
end

--- Останавливает узел и убирает его каталог.
helper.stop_node = testing.stop_node

--- Сверяет, что каждый вызов бросает названный отказ и винит строку
--- вызова в файле проверок, а не внутри пакета.
---
--- Вызов стоит в замыкании первой строкой тела, то есть строкой ниже
--- слова `function`: место броска сверяется с ней целиком — файлом,
--- строкой и текстом.
---@param cases table[] Пары: замыкание с вызовом и текст броска
function helper.assert_blamed(cases)
    for _, case in ipairs(cases) do
        local _, err = pcall(case[1])
        local info = debug.getinfo(case[1], 'S') --[[@as { short_src: string, linedefined: integer }]]

        t.assert_equals(err, ('%s:%d: %s'):format(info.short_src, info.linedefined + 1, case[2]))
    end
end

--- Значение мимо проверки типов: негодный аргумент нарочно.
---@param value any
---@return any
function helper.wrong(value)
    return value
end

--- Возвращает шину в исходное: слушатели сняты, счёт забыт, крюки сняты,
--- внешние зависимости настоящие, журнал забыт.
function helper.forget()
    helper.event.reset()
    helper.event._set_source(nil)
    helper.series._set_source(nil)

    for _, name in ipairs(helper.event.hooks()) do
        helper.event.hook(name, nil)
    end

    helper.journal.forget()
end

--- Настройки слушателя с умолчаниями у этой шины — для слушателя
--- в проверках; негодные винят строку того, кто позвал помощника.
---@param opts table|nil
---@return TntEventPolicy
function helper.policy(opts)
    return helper.listener.policy(opts, helper.event.features, 2)
end

--- Тело: цепочка из `levels` вложенных таблиц под корнем — всего таблиц
--- на одну больше.
---@param levels integer
---@return table
function helper.deep(levels)
    local root = {}
    local node = root

    for _ = 1, levels do
        node.next = {}
        node = node.next
    end

    return root
end

--- Ждёт, пока условие сбудется; иначе валит проверку с поясняющим текстом.
---@param about string Чего ждали
---@param predicate fun(): boolean
function helper.until_true(about, predicate)
    t.helpers.retrying({ timeout = 5, delay = 0.01 }, function()
        assert(predicate(), about)
    end)
end

return helper
