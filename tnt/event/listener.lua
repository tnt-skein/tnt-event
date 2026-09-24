--- Слушатель шины: очередь в памяти, работники, итог обработчика.
---
--- **Очередь слушателя — в памяти, и полная отказывает сразу**, а не ждёт:
--- `dispatch` зовут и из `box.on_commit`, где уступка роняет процесс.
--- Потолок очереди делится между полосами работников — работник берёт
--- из своей полосы.
---
--- **Ключ — полоса, полоса — работник**: пока работник занят
--- событием ключа, следующее того же ключа стоит в его полосе, и порядок
--- внутри ключа держится при любом числе работников. Событие без ключа
--- идёт по своему опознавателю: с общим ключом все безключевые встали бы
--- одной полосой, и `workers` ничего бы не дали.
---
--- **Отсрочка и повтор — сон работника**: пока событие ждёт, полоса
--- стоит. Порядок важнее скорости, а слушателю, которому нужна скорость,
--- объявляют `workers` и ключ. Остановка будит спящего работника сразу.
---
--- **Гарантия — не больше раза**: что лежит в полосах, пропадает
--- с процессом и с остановкой слушателя, и это считается (`dropped`).
---
--- **Итоги считаются вместе с рядами**: подтверждение, повтор, зарытие
--- и сбой шага растут в счётчиках `status()` и в общих рядах договора
--- одним вызовом. Что работник держит сейчас — событие в обработке либо
--- в отсрочке, — слушатель помнит по работнику: из этого ряд глубины
--- берёт состояния `taken` и `delayed`.
---
--- Обработчик — код приложения, и права у него — права фонового файбера,
--- то есть процесса, а не того, кто объявил слушателя. Внутри чужой
--- транзакции он не зовётся: у работника свой файбер.

local digest = require('digest')
local fiber = require('fiber')

local message = require('tnt.message')
local must = require('tnt.must')
local series = require('tnt.message.series')

local log = require('tnt.log').new('tnt.event')

--- Крюки отправки и обработки: список шины, общий с фасадом.
local hooks = message.hooks('tnt.event')

--- Сбой шага пишется с подавлением повторов: беда, которая держится,
--- иначе писала бы строку на каждом событии.
local steps = require('tnt.log').changes('tnt.event')

local Module = {}

--- Сколько событий ждёт слушателя по умолчанию, пока `dispatch`
--- не начнёт отказывать.
Module.CAPACITY = 1000

--- Как слушатель называет свои настройки и чего не умеет.
local LISTENING = {
    what = 'настройки слушателя',
    own = { capacity = '?integer' },
    unable = {
        atomic = 'шина событий не умеет: обработчик идёт своим файбером после фиксации, '
            .. 'и транзакции вызывающего там уже нет',
    },
}

---@class TntEventPolicy : TntMessagePolicy Настройки слушателя с умолчаниями
---@field capacity integer Потолок очереди слушателя

---@class TntEventSink Куда слушатель отдаёт то, что решает не он
---@field bury fun(envelope: TntMessage, reason: string) Зарыть событие
---@field withdraw fun(listener: TntEventListener) Снять слушателя с шины

---@class TntEventListener Слушатель: `stop()` перестаёт брать новые события
---@field name string Имя события
---@field handler fun(message: TntMessage): any, any
---@field invoke TntMessageInvoke Исполнение обработчика
---@field policy TntEventPolicy
---@field sink TntEventSink
---@field lanes table[] Полосы работников: по каналу на работника
---@field counts table<string, integer> Счётчики слушателя
---@field stopping boolean Попросили остановиться
---@field wake table Ожидание, которое будит остановка
---@field workers integer Сколько работников ещё живо
---@field holding table<integer, string> Что держит работник по номеру: `taken` — обработка, `delayed` — отсрочка
local Listener = {}
Listener.__index = Listener

--- Проверяет настройки слушателя и ставит умолчания; негодные —
--- исключение на строке того, кто объявил слушателя.
---
--- Общие настройки получателя — у договора, а потолок очереди — свой:
--- очередь слушателя живёт в памяти, и без потолка медленный слушатель
--- съел бы память узла.
---@param opts any
---@param features table<string, boolean> Признаки шины
---@param level integer Уровень вины в кадрах вызывающего эту функцию
---@return TntEventPolicy
function Module.policy(opts, features, level)
    local common, given = message.policy(opts, features, level + 1, LISTENING)
    local policy = common --[[@as TntEventPolicy]]

    must.at(level + 1).optional.positive(given.capacity, 'настройки слушателя.capacity')
    policy.capacity = given.capacity or Module.CAPACITY

    return policy
end

--- Полоса события: её выбирает ключ, а у безключевого — опознаватель.
---@param listener TntEventListener
---@param envelope TntMessage
---@return integer
local function lane_of(listener, envelope)
    return digest.crc32(envelope.key or envelope.id) % #listener.lanes + 1
end

--- Ждёт `seconds` и говорит, остановили ли слушателя за это время.
---
--- Ожидание — по условию, а не сном: остановка будит его сразу, и работник
--- не держит узел лишнюю минуту отсрочки.
---@param listener TntEventListener
---@param seconds number
---@return boolean stopped
local function waited(listener, seconds)
    listener.wake:wait(seconds)

    return listener.stopping
end

--- Отмечает, что работник держит событие и в каком состоянии.
---@param listener TntEventListener
---@param number integer Номер работника
---@param state string `taken` — в обработке, `delayed` — в отсрочке
local function hold(listener, number, state)
    listener.holding[number] = state
end

--- Считает событие потерянным: слушателя остановили, пока оно ждало.
---@param listener TntEventListener
---@param envelope TntMessage
local function lost(listener, envelope)
    listener.counts.dropped = listener.counts.dropped + 1

    log.warn('событие пропало с остановкой слушателя', {
        destination = listener.name,
        id = envelope.id,
        attempt = envelope.attempt,
    })
end

--- Записывает итог, который больше не повторяется.
---@param listener TntEventListener
---@param envelope TntMessage
---@param verdict string
---@param reason string|nil Причина у `bury`
local function settled(listener, envelope, verdict, reason)
    series.count(listener.counts, listener.name, verdict)

    if verdict == message.ACK then
        return
    end

    listener.sink.bury(envelope, reason --[[@as string]])
    log.error('событие зарыто', {
        destination = listener.name,
        id = envelope.id,
        attempt = envelope.attempt,
        err = reason,
    })
end

--- Ведёт событие до итога: отсрочка, выдачи, повторы.
---
--- Ожидание первой выдачи меряется после отсрочки: отсрочка — часть
--- ожидания, как у очереди, где сообщение с отсрочкой лежит невыданным.
---@param listener TntEventListener
---@param item { message: TntMessage, delay: number|nil }
---@param number integer Номер работника
local function handled(listener, item, number)
    local envelope = item.message

    if item.delay ~= nil then
        hold(listener, number, 'delayed')

        if waited(listener, item.delay) then
            return lost(listener, envelope)
        end
    end

    while true do
        hold(listener, number, 'taken')
        envelope.attempt = envelope.attempt + 1
        series.taken(listener.name, envelope)

        -- Область контекста отправителя, крюки обработки, приговор.
        local verdict, extra = message.judge(hooks, log, envelope, listener.policy, listener.invoke)

        if verdict ~= message.RETRY then
            return settled(listener, envelope, verdict, extra --[[@as string]])
        end

        local pause = extra --[[@as number]]

        series.count(listener.counts, listener.name, message.RETRY)
        hold(listener, number, 'delayed')
        log.debug('событие уйдёт обработчику снова', {
            destination = listener.name,
            id = envelope.id,
            attempt = envelope.attempt,
            delay = pause,
        })

        if waited(listener, pause) then
            return lost(listener, envelope)
        end
    end
end

--- Тело работника: события своей полосы до её закрытия.
---
--- Закрытая полоса будит ждущего сразу и больше ничего не отдаёт: то, что
--- в ней осталось, сосчитано остановкой и пропало. Сбой шага считается
--- и пишется, работник живёт дальше — беда одного события не уносит
--- остальные. Отметка «что держит работник» снимается после шага, чем бы
--- он ни кончился: сорвавшийся шаг иначе числился бы в глубине вечно.
---@param listener TntEventListener
---@param lane table Канал полосы
---@param number integer Номер работника
local function work(listener, lane, number)
    fiber.self():name(('event/%s/%d'):format(listener.name, number), { truncate = true })

    while not lane:is_closed() do
        local item = lane:get()

        if item ~= nil then
            local ok, err = pcall(handled, listener, item, number)

            listener.holding[number] = nil

            if not ok then
                series.count(listener.counts, listener.name, 'failures')
                steps.warn('шаг работника слушателя сорвался', {
                    destination = listener.name,
                    err = tostring(err),
                })
            end
        end
    end

    listener.workers = listener.workers - 1
end

--- Заводит слушателя и его работников.
---@param name string Имя события
---@param handler fun(message: TntMessage): any, any
---@param policy TntEventPolicy
---@param sink TntEventSink
---@return TntEventListener
function Module.start(name, handler, policy, sink)
    local listener = setmetatable({
        name = name,
        handler = handler,
        invoke = message.invoker(handler),
        policy = policy,
        sink = sink,
        lanes = {},
        counts = { ack = 0, retry = 0, bury = 0, dropped = 0, failures = 0 },
        stopping = false,
        wake = fiber.cond(),
        workers = policy.workers,
        holding = {},
    }, Listener)

    -- Потолок очереди делится между полосами: общий потолок на всех дал бы
    -- одному ключу занять очередь целиком, а работник всё равно берёт
    -- только из своей полосы.
    local depth = math.ceil(policy.capacity / policy.workers)

    for number = 1, policy.workers do
        local lane = fiber.channel(depth)

        table.insert(listener.lanes, lane)
        fiber.new(work, listener, lane, number)
    end

    return listener
end

--- Отказ укладки: событие до обработчика не дошло, и это считается.
---@param listener TntEventListener
---@param why string
---@return boolean placed
---@return string why
local function refused(listener, why)
    listener.counts.dropped = listener.counts.dropped + 1

    return false, why
end

--- Кладёт событие в полосу его ключа; полная полоса — отказ с причиной.
---
--- Место проверяется до укладки, а укладка идёт без срока: уступать здесь
--- нельзя — событие кладут и из `box.on_commit`, где уступка роняет
--- процесс, — а в полосу, где место есть, `put` не ждёт ни мгновения.
---@param envelope TntMessage
---@param delay number|nil Отсрочка первой выдачи, секунды; пусто — сразу
---@return boolean placed
---@return string|nil why Почему не поместилось
function Listener:offer(envelope, delay)
    local lane = self.lanes[lane_of(self, envelope)] --[[@as table]]

    -- Остановленного шина уже сняла, и сюда попадает только тот, кто держит
    -- слушателя сам: закрытая полоса приняла бы событие молча.
    if lane:is_closed() then
        return refused(self, ('слушатель %s остановлен'):format(self.name))
    end

    if lane:is_full() then
        local full = ('очередь слушателя %s полна: %d событий в полосе'):format(
            self.name,
            lane:count()
        )

        return refused(self, full)
    end

    lane:put({ message = envelope, delay = delay })

    return true
end

--- Перестаёт брать новые события; взятое дорабатывается.
---
--- То, что лежит в полосах, пропадает — гарантия шины «не больше раза», —
--- и пропавшее считается. Спящий работник просыпается сразу и своё событие
--- бросает. Повторная остановка ничего не делает: оставшееся уже сосчитано.
function Listener:stop()
    if self.stopping then
        return
    end

    self.stopping = true
    self.wake:broadcast()

    for _, lane in ipairs(self.lanes) do
        self.counts.dropped = self.counts.dropped + lane:count()
        lane:close()
    end

    self.sink.withdraw(self)
end

--- Что со слушателем сейчас: работники, потолок, глубина, счётчики.
---
--- `depth` — сколько ждёт в полосах; `taken` и `delayed` — сколько
--- работников держит событие в обработке и в отсрочке.
---@return table
function Listener:status()
    local depth = 0

    for _, lane in ipairs(self.lanes) do
        depth = depth + lane:count()
    end

    local held = { taken = 0, delayed = 0 }

    for _, state in pairs(self.holding) do
        held[state] = held[state] --[[@as integer]] + 1
    end

    local counts = {}

    for name, value in pairs(self.counts) do
        counts[name] = value
    end

    return {
        name = self.name,
        workers = self.workers,
        capacity = self.policy.capacity,
        depth = depth,
        taken = held.taken,
        delayed = held.delayed,
        counts = counts,
    }
end

return Module
