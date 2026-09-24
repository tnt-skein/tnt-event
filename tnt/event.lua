--- Шина событий внутри узла: работа идёт на файберах этого же узла, без
--- спейсов, брокеров и сети.
---
---     local event = require('tnt.event')
---
---     event.listen('order.paid', function(message)
---         local ok, reason = notify(message.body.order)
---
---         if not ok then
---             return nil, reason           -- повтор с отсрочкой, потом зарыть
---         end
---     end, { workers = 2 })
---
---     local sent, err = event.dispatch('order.paid', { order = 11 })
---
--- **Договор общий с очередями**: конверт с опознавателем отправителя,
--- подтверждение итогом обработчика, контекст заголовками сообщения, отказ
--- `TntStorageFailure`. Обработчик, написанный здесь, без правки переезжает
--- на надёжную очередь с тем же конвертом — ради этого договор и держится.
---
--- **Гарантия — не больше раза**: очередь слушателя, отсрочка и зарытые
--- живут в памяти и пропадают с процессом. Надёжное — очередь в спейсе
--- или ящик исходящих.
---
--- **В транзакции событие уходит после фиксации**: конверт собирается
--- в миг вызова, с контекстом вызывающего, а в очередь слушателя ложится
--- из `box.on_commit`; при откате его нет. Внутри триггера — только укладка
--- без ожидания: уступка там роняет процесс, поэтому полная очередь
--- слушателя отказывает сразу, а не ждёт места.
---
--- **Синхронного вызова слушателей нет**: позвать функцию приложение умеет
--- и без шины, а синхронный слушатель внутри транзакции рвал бы её первой
--- же уступкой.
---
--- **Ряды метрик — общие ряды договора** (`tnt.message.series`): имя события
--- идёт меткой `destination`, и шина видна в выкладке теми же рядами
--- `message_*`, что и очередь, на которую переедет её обработчик.
---
--- Зависимость от пакета приходит аргументом, внешняя — через `tnt-external`:
--- слушатель — значение `listen`, а не запись в контейнере, а `box` шина
--- трогает как внешнюю зависимость.

local clock = require('tnt.clock')
local message = require('tnt.message')
local must = require('tnt.must')
local external = require('tnt.external')
local series = require('tnt.message.series')

local failure = require('tnt.storage.failure')
local listener_of = require('tnt.event.listener')

local log = require('tnt.log').new('tnt.event')

--- Крюки отправки и обработки: список шины, общий со слушателями.
local hooks = message.hooks('tnt.event')

local Module = {}

--- Сколько зарытых событий помнится: список в памяти, старые вытесняются
--- со счётом. Зарытое здесь не пережидает перезапуск — зарытому, которое
--- обязано дожить до человека, место в очереди, что хранит зарытые в спейсе.
Module.DEAD = 100

--- Что умеет шина. Настройка, которой она не умеет, — исключение, а не
--- молчаливый пропуск.
Module.features = {
    delay = true,
    ttl = false,
    ttr = false,
    touch = false,
    priority = false,
    key = true,
    transactional = true,
    atomic = false,
}

--- Ставит, заменяет или снимает крюк отправки и обработки.
Module.hook = hooks.set

--- Имена поставленных крюков по порядку.
Module.hooks = hooks.names

--- Чего шина не умеет и почему: об этом говорит отказ отправки.
local SENDING = {
    unable = {
        priority = 'шина событий не умеет: очередь слушателя выдаёт по порядку прихода',
        ttl = 'шина событий не умеет: событие ждёт слушателя, пока жив процесс, '
            .. 'и сроку жизни неоткуда взяться',
    },
}

--- Имя события: буква, дальше буквы, цифры, подчёркивание, точка и дефис —
--- `order.paid`, «предмет и что с ним случилось». Спейсов у шины нет, и имя
--- ничем, кроме журнала и `status()`, не ограничено.
local NAME = '^%a[%w_.-]*$'

--- Внешние средства: транзакция вызывающего и её фиксация.
local source = external.install(Module, {
    -- До `box.cfg` любой вопрос к box — исключение «Please call box.cfg{}
    -- first»: узла нет, значит нет и транзакции.
    in_txn = function()
        return type(box.cfg) ~= 'function' and box.is_in_txn()
    end,

    on_commit = function(task)
        box.on_commit(task)
    end,
})

--- Слушатели по именам событий.
---@type table<string, TntEventListener[]>
local listeners = {}

--- Счётчики шины с нуля: отправлено, отказано переполнением,
--- вытеснено из зарытых.
---
--- Одним местом, а не двумя: сброс ставит те же нули, что и запуск, — два
--- перечисления однажды разошлись бы.
---@return table<string, integer>
local function zeroed()
    return { sent = 0, refused = 0, evicted = 0 }
end

--- Счётчики шины.
local counts = zeroed()

--- Зарытые события: последние `Module.DEAD`.
---@type table[]
local dead = {}

--- Все слушатели узла одним списком: снимок, по которому можно менять
--- сам список — остановка снимает слушателя с шины.
---@return TntEventListener[]
local function standing()
    local all = {}

    for _, named in pairs(listeners) do
        for _, one in ipairs(named) do
            table.insert(all, one)
        end
    end

    return all
end

--- Зарывает событие: запись в список, старые вытесняются со счётом.
---@param envelope TntMessage
---@param reason string
local function bury(envelope, reason)
    table.insert(dead, {
        id = envelope.id,
        name = envelope.name,
        reason = reason,
        attempt = envelope.attempt,
        buried = clock.realtime(),
    })

    if #dead > Module.DEAD then
        table.remove(dead, 1)
        counts.evicted = counts.evicted + 1
    end
end

--- Снимает остановленного слушателя с шины: события ему больше не идут.
---
--- Соседи его имени остаются на месте: слушателей у события бывает
--- несколько, и остановка одного не лишает событий остальных.
---@param listener TntEventListener
local function withdraw(listener)
    local named = listeners[listener.name] or {}

    for index, known in ipairs(named) do
        if known == listener then
            table.remove(named, index)

            break
        end
    end
end

--- Куда слушатель отдаёт то, что решает не он.
---@type TntEventSink
local sink = { bury = bury, withdraw = withdraw }

--- Кладёт событие всем слушателям имени; каждому — своя копия.
---
--- Без уступки: зовётся и из `box.on_commit`. Полный слушатель не отнимает
--- событие у остальных — очередь у каждого своя, — а отказ говорит
--- о первом полном.
---@param envelope TntMessage
---@param delay number|nil
---@return string|nil id
---@return TntStorageFailure|nil err
local function deliver(envelope, delay)
    local complaint = nil

    for _, one in ipairs(listeners[envelope.name] or {}) do
        local copy = table.deepcopy(envelope) --[[@as TntMessage]]
        local placed, why = one:offer(copy, delay)

        if not placed then
            -- Причину называет первый полный: остальные полные — та же беда.
            complaint = complaint or why
        end
    end

    if complaint ~= nil then
        local err = failure.new('overflow', complaint --[[@as string]], { sent = false })

        counts.refused = counts.refused + 1
        series.refused(envelope.name, err)

        return nil, err
    end

    series.count(counts, envelope.name, 'sent')

    return envelope.id
end

--- Объявляет слушателя события.
---
--- У каждого слушателя своя очередь в памяти и свои работники: два
--- слушателя одного имени не мешают друг другу, и медленный не держит
--- быстрого.
---@param name string Имя события
---@param handler fun(message: TntMessage): any, any
---@param opts table|nil Настройки слушателя
---@return TntEventListener
function Module.listen(name, handler, opts)
    local caller = must.at(2)

    caller.matches(name, 'имя события', NAME)
    caller.callable(handler, 'обработчик')

    local policy = listener_of.policy(opts, Module.features, 2)
    local listener = listener_of.start(name, handler, policy, sink)

    listeners[name] = listeners[name] or {}
    table.insert(listeners[name], listener)

    return listener
end

--- Рассылает событие слушателям и отдаёт его опознаватель.
---
--- Опознаватель приходит, когда событие легло в очереди слушателей;
--- в транзакции — сразу, а укладка идёт после фиксации, и отказ полной
--- очереди там уже некому отдать: он остаётся записью `warn`. Событие,
--- которого никто не слушает, — не отказ: шина не знает, кому оно было
--- нужно.
---@param name string Имя события
---@param body any Тело — простые данные
---@param opts TntMessageSendOptions|nil
---@return string|nil id
---@return TntStorageFailure|nil err
function Module.dispatch(name, body, opts)
    must.at(2).matches(name, 'имя события', NAME)

    local given = message.options(opts, Module.features, 2, SENDING)

    message.body(body, 2)

    local call = { kind = message.SEND, name = name }

    return hooks.around(call, function()
        local envelope = message.envelope(name, body, given)

        call.message = envelope

        if not source().in_txn() then
            -- Крюк отдаёт итог отправки как есть: пару «опознаватель, отказ».
            ---@diagnostic disable-next-line: redundant-return-value
            return deliver(envelope, given.delay)
        end

        source().on_commit(function()
            local placed, why = deliver(envelope, given.delay)

            if placed == nil then
                log.warn('событие после фиксации не поместилось и пропало', {
                    destination = name,
                    id = envelope.id,
                    err = tostring(why),
                })
            end
        end)

        return envelope.id
    end)
end

--- Зарытые события: копия списка, от старых к новым.
---@return table[]
function Module.dead()
    return table.deepcopy(dead)
end

--- Что с шиной сейчас: счётчики, слушатели по именам, зарытые, крюки.
---
--- Слушатели — таблицей по именам событий, а списком внутри имени: список
--- всех разом пришлось бы упорядочивать, а порядок `pairs` от запуска
--- к запуску разный, и сводка узла выходила бы каждый раз иной.
---@return table
function Module.status()
    local totals = {}

    for name, value in pairs(counts) do
        totals[name] = value
    end

    local shown = {}

    for name, named in pairs(listeners) do
        local each = {}

        for _, one in ipairs(named) do
            table.insert(each, one:status())
        end

        -- Имя, у которого слушателей больше нет, в сводке не показывается:
        -- пустой список — не то же, что слушатель, и читающего он путал бы.
        if each[1] ~= nil then
            shown[name] = each
        end
    end

    return { counts = totals, listeners = shown, dead = #dead, hooks = hooks.names() }
end

--- Глубина шины по именам событий: сколько ждёт в полосах, сколько
--- у работников в обработке и в отсрочке, сколько зарыто.
---
--- Слушатели одного имени складываются: у каждого своя очередь, и событие,
--- разосланное двоим, лежит двумя копиями. Имя, у которого нет ни
--- слушателей, ни зарытых, в ряду не показывается — как и в сводке.
---@return table<string, table<string, integer>>
local function measured()
    local depths = {}

    local function of(name)
        local depth = depths[name] or { ready = 0, taken = 0, delayed = 0, dead = 0 }

        depths[name] = depth

        return depth
    end

    for _, one in ipairs(standing()) do
        local shown = one:status()
        local depth = of(shown.name)

        depth.ready = depth.ready + shown.depth
        depth.taken = depth.taken + shown.taken
        depth.delayed = depth.delayed + shown.delayed
    end

    for _, buried in ipairs(dead) do
        local depth = of(buried.name)

        depth.dead = depth.dead + 1
    end

    return depths
end

series.depth('tnt.event', measured)

--- Снимает всех слушателей и забывает счёт.
---
--- Нужен проверкам и перезагрузке кода: слушатель, объявленный заново,
--- иначе встал бы вторым, и событие пришло бы обработчику дважды. Крюки
--- живут своей жизнью — их снимает `event.hook(имя, nil)`.
function Module.reset()
    for _, one in ipairs(standing()) do
        one:stop()
    end

    listeners = {}
    dead = {}
    counts = zeroed()
end

return Module
