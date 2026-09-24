--- Слушатель: выдача обработчику, повтор, зарытые, полосы ключей,
--- переполнение и остановка.
---
--- Файберы, каналы и контекст здесь настоящие: шина живёт в памяти, и
--- двойник показал бы только то, что мы правильно разговариваем сами
--- с собой. Двойником приходит одно — приёмник зарытых и снятия
--- (`sink`): его держит фасад, а слушателю он нужен аргументом.

local clock = require('clock')
local fiber = require('fiber')
local t = require('luatest')

local helper = dofile('test/helper.lua')

local context = helper.context
local listener_of = helper.listener

local g = t.group('tnt.event.listener')

--- Заведённые слушатели: их останавливает `after_each`, иначе работники
--- соседней проверки достались бы от прежней.
local running = {}

--- Что слушатель отдал приёмнику.
local given = { buried = {}, withdrawn = {} }

g.before_each(function()
    running, given = {}, { buried = {}, withdrawn = {} }
    helper.forget()
end)

g.after_each(function()
    for _, listener in ipairs(running) do
        listener:stop()
    end
end)

g.after_all(function()
    helper.journal.release()
end)

--- Приёмник зарытых и снятий: то, что решает не слушатель.
---@type TntEventSink
local sink = {
    bury = function(envelope, reason)
        table.insert(given.buried, { id = envelope.id, attempt = envelope.attempt, reason = reason })
    end,
    withdraw = function(listener)
        table.insert(given.withdrawn, listener.name)
    end,
}

--- Конверт события для полосы.
---@param body any
---@param opts table|nil Поля конверта поверх умолчаний
---@return TntMessage
local function envelope_of(body, opts)
    local envelope = {
        id = '01J0000000000000000000000X',
        name = 'order.paid',
        body = body,
        headers = {},
        attempt = 0,
        created = 0,
    }

    for field, value in pairs(opts or {}) do
        envelope[field] = value
    end

    return envelope --[[@as TntMessage]]
end

--- Заводит слушателя с настройками проверки.
---@param handler function
---@param opts table|nil
---@param name string|nil Имя события; по умолчанию `order.paid`
---@return TntEventListener
local function listening(handler, opts, name)
    local listener = listener_of.start(name or 'order.paid', handler, helper.policy(opts), sink)

    table.insert(running, listener)

    return listener
end

g.test_the_handler_gets_the_envelope_and_the_context_of_the_sender = function()
    local seen = {}
    local listener = listening(function(message)
        table.insert(seen, {
            id = message.id,
            body = message.body,
            attempt = message.attempt,
            request_id = context.get('request_id'),
        })
    end)

    listener:offer(envelope_of({ order = 11 }, { headers = { ['x-request-id'] = 'r-7' } }))
    helper.until_true('событие не обработано', function()
        return #seen == 1
    end)

    t.assert_equals(seen[1].body, { order = 11 })
    t.assert_equals(seen[1].attempt, 1, 'первая выдача — первая')
    t.assert_equals(
        seen[1].request_id,
        'r-7',
        'опознаватель запроса доехал заголовком'
    )
    t.assert_equals(listener:status().counts.ack, 1, 'ничего не вернул — подтверждено')
end

g.test_a_message_without_a_request_id_gets_its_own = function()
    local seen = nil
    local listener = listening(function()
        seen = context.get('request_id')
    end)

    listener:offer(envelope_of('без контекста'))
    helper.until_true('событие не обработано', function()
        return seen ~= nil
    end)

    t.assert_equals(
        #seen,
        26,
        'свой ULID: запись обработчика без опознавателя в журнале не найти'
    )
end

g.test_a_refusal_comes_back_with_a_pause_and_then_is_buried = function()
    local attempts = {}
    local listener = listening(function(message)
        table.insert(attempts, { attempt = message.attempt, at = clock.monotonic() })

        return nil, 'служба молчит'
    end, { max_attempts = 3, backoff = { base = 0.05, max = 1 } })

    listener:offer(envelope_of('работа'))
    helper.until_true('событие не зарыто', function()
        return #given.buried == 1
    end)

    t.assert_equals(#attempts, 3, 'три выдачи, потом зарыто')
    t.assert_equals(attempts[3].attempt, 3)
    t.assert_almost_equals(attempts[2].at - attempts[1].at, 0.05, 0.04)
    t.assert_almost_equals(attempts[3].at - attempts[2].at, 0.1, 0.05)
    t.assert_equals(given.buried[1].reason, 'попыток 3 из 3: служба молчит')
    t.assert_equals(listener:status().counts, {
        ack = 0,
        retry = 2,
        bury = 1,
        dropped = 0,
        failures = 0,
    })

    local entry = helper.journal.find('событие зарыто')

    t.assert_equals(entry.record.fields.destination, 'order.paid')
    t.assert_equals(entry.record.fields.attempt, 3)
    t.assert_equals(entry.level, 'error')
    t.assert_equals(helper.journal.find('событие уйдёт обработчику снова').level, 'debug')
end

g.test_an_unrepeatable_refusal_is_buried_at_the_first_attempt = function()
    local attempts = 0
    local listener = listening(function()
        attempts = attempts + 1

        return nil, { retriable = false, message = 'тело не прошло проверку' }
    end)

    listener:offer(envelope_of('негодное'))
    helper.until_true('событие не зарыто', function()
        return #given.buried == 1
    end)

    t.assert_equals(attempts, 1, 'повтор негодного тела ничего не даст')
    t.assert_equals(given.buried[1].reason, 'тело не прошло проверку')
    t.assert_equals(given.buried[1].attempt, 1)
end

g.test_a_handler_that_raised_is_written_with_its_stack = function()
    local listener = listening(function()
        error('деление на ноль')
    end, { max_attempts = 1 })

    listener:offer(envelope_of('боом'))
    helper.until_true('событие не зарыто', function()
        return #given.buried == 1
    end)

    local record = helper.journal.find('обработчик сообщения бросил').record
    local first = record.fields.traceback:match('stack traceback:\n\t([^\n]+)')

    t.assert_str_contains(record.fields.err, 'деление на ноль')
    t.assert_equals(
        first,
        "[C]: in function 'error'",
        'стек начинается с места броска: кадры самого слушателя в нём не нужны'
    )
    t.assert_str_contains(given.buried[1].reason, 'попыток 1 из 1: ')
    t.assert_str_contains(given.buried[1].reason, 'деление на ноль')
end

g.test_a_step_that_slipped_is_counted_and_the_worker_lives_on = function()
    local unreadable = setmetatable({}, {
        __tostring = function()
            error('причина не читается', 0)
        end,
    })
    local seen = {}
    local listener = listening(function(message)
        table.insert(seen, message.body)

        if message.body == 'битое' then
            return nil, unreadable
        end
    end, { max_attempts = 1 })

    listener:offer(envelope_of('битое'))
    helper.until_true('сбой не сосчитан', function()
        return listener:status().counts.failures == 1
    end)

    listener:offer(envelope_of('следующее'))
    helper.until_true('работник не взял следующее', function()
        return #seen == 2
    end)

    t.assert_equals(seen, { 'битое', 'следующее' })
    t.assert_equals(#given.buried, 0, 'сорвавшийся шаг событие не зарыл')
    t.assert_str_contains(
        helper.journal.find('шаг работника слушателя сорвался').record.fields.err,
        'причина не читается'
    )
end

g.test_the_order_inside_a_key_holds_with_two_workers = function()
    local seen = {}
    local listener = listening(function(message)
        table.insert(seen, message.body .. ' взято')
        fiber.sleep(0.02)
        table.insert(seen, message.body .. ' сделано')
    end, { workers = 2 })

    -- Ключ «a» и ключ «c» ложатся в разные полосы (crc32 % 2), поэтому
    -- одинаковые ключи идут по одному, а разные — разом.
    listener:offer(envelope_of('a1', { key = 'a' }))
    listener:offer(envelope_of('a2', { key = 'a' }))
    listener:offer(envelope_of('c1', { key = 'c' }))

    helper.until_true('обработаны не все', function()
        return #seen == 6
    end)

    local order = table.concat(seen, ' ')

    t.assert_equals(order:find('a2 взято') > order:find('a1 сделано'), true, 'a2 ждал a1')
    t.assert_equals(
        order:find('a2 сделано') > order:find('a2 взято'),
        true,
        'a2 доделано последним'
    )
    t.assert_equals(
        order:find('c1 взято') < order:find('a1 сделано'),
        true,
        'ключ c пошёл разом'
    )
end

g.test_messages_without_a_key_spread_across_the_workers = function()
    local busy = fiber.cond()
    local seen = {}
    local listener = listening(function(message)
        table.insert(seen, message.body)
        busy:wait(1)
    end, { workers = 2 })

    -- Опознаватель за ключ: с общим ключом безключевые встали бы одной
    -- полосой, и второй работник простоял бы.
    listener:offer(envelope_of('первое', { id = 'x' }))
    listener:offer(envelope_of('второе', { id = 'a' }))

    helper.until_true('взято не двумя работниками', function()
        return #seen == 2
    end)

    busy:broadcast()
    t.assert_items_equals(seen, { 'первое', 'второе' })
end

g.test_a_full_lane_refuses_at_once_and_counts_the_loss = function()
    local busy = fiber.cond()
    local listener = listening(function()
        busy:wait(1)
    end, { capacity = 2 })

    listener:offer(envelope_of('в работе'))
    helper.until_true('первое не взято', function()
        return listener:status().counts.ack == 0 and listener:status().depth == 0
    end)

    local placed = { listener:offer(envelope_of('первое в очереди')) }
    local queued = { listener:offer(envelope_of('второе в очереди')) }
    local refused, why = listener:offer(envelope_of('лишнее'))

    t.assert_equals({ placed[1], queued[1] }, { true, true }, 'потолок очереди — два события')
    t.assert_equals(refused, false, 'ждать некогда: dispatch зовут и из on_commit')
    t.assert_equals(why, 'очередь слушателя order.paid полна: 2 событий в полосе')
    t.assert_equals(listener:status().counts.dropped, 1)
    t.assert_equals(listener:status().depth, 2)

    busy:broadcast()
end

g.test_the_capacity_is_split_between_the_lanes = function()
    local listener = listening(function() end, { workers = 3, capacity = 7 })

    t.assert_equals(#listener.lanes, 3)
    t.assert_equals(
        assert(listener.lanes[1]):size(),
        3,
        'потолок делится с округлением вверх'
    )
    t.assert_equals(
        listener:status().capacity,
        7,
        'в сводке — потолок слушателя целиком'
    )
end

g.test_stopping_drops_what_is_left_and_the_workers_go_away = function()
    local busy = fiber.cond()
    local listener = listening(function()
        busy:wait(1)
    end)

    listener:offer(envelope_of('в работе'))
    helper.until_true('первое не взято', function()
        return listener:status().depth == 0
    end)

    listener:offer(envelope_of('останется в очереди'))
    listener:stop()

    t.assert_equals(listener:status().counts.dropped, 1, 'то, что в полосе, пропало')
    t.assert_equals(given.withdrawn, { 'order.paid' }, 'слушатель снят с шины')

    busy:broadcast()
    helper.until_true('работник не кончился', function()
        return listener:status().workers == 0
    end)
end

g.test_a_delayed_message_waits_and_holds_its_lane = function()
    local seen = {}
    local listener = listening(function(message)
        table.insert(seen, message.body)
    end)

    listener:offer(envelope_of('через миг'), 0.2)
    listener:offer(envelope_of('за ним'))
    fiber.sleep(0.05)

    t.assert_equals(
        seen,
        {},
        'отсрочка держит полосу: порядок важнее скорости'
    )

    helper.until_true('отсроченное не обработано', function()
        return #seen == 2
    end)

    t.assert_equals(seen, { 'через миг', 'за ним' })
end

g.test_stopping_wakes_a_waiting_worker_and_the_message_is_lost = function()
    local listener = listening(function() end)

    listener:offer(envelope_of('ждёт отсрочки'), 60)
    fiber.sleep(0.05)
    listener:stop()

    helper.until_true('работник не проснулся', function()
        return listener:status().workers == 0
    end)

    t.assert_equals(listener:status().counts.dropped, 1, 'не больше раза: событие пропало')
    t.assert_equals(
        helper.journal.find('событие пропало с остановкой слушателя').record.fields.attempt,
        0,
        'до первой выдачи так и не дошло'
    )
end

g.test_stopping_during_a_pause_drops_the_message_it_was_repeating = function()
    local attempts = 0
    local listener = listening(function()
        attempts = attempts + 1

        return nil, 'служба молчит'
    end, { backoff = { base = 60, max = 60 } })

    listener:offer(envelope_of('повторяется'))
    helper.until_true('первая выдача не случилась', function()
        return attempts == 1
    end)

    listener:stop()
    helper.until_true('работник не проснулся', function()
        return listener:status().workers == 0
    end)

    t.assert_equals(attempts, 1, 'вторая выдача не пошла')
    t.assert_equals(#given.buried, 0, 'остановка не зарывает')
    t.assert_equals(
        helper.journal.find('событие пропало с остановкой слушателя').record.fields.attempt,
        1
    )
end

g.test_a_worker_is_named_after_the_listener_and_its_number = function()
    -- Своё имя события: работники соседних проверок ещё доделывают взятое,
    -- и в `fiber.info()` они стоят рядом.
    local listener = listening(function() end, { workers = 2 }, 'order.named')
    local named = {}

    -- Имя работника видно в `fiber.info()`: по нему его и находят на узле,
    -- где файберов сотни. Ставится оно, когда работник впервые пошёл.
    helper.until_true('работники не назвались', function()
        named = {}

        for _, one in pairs(fiber.info({ backtrace = false })) do
            if (one.name or ''):find('order.named', 1, true) ~= nil then
                table.insert(named, one.name)
            end
        end

        return #named == 2
    end)

    table.sort(named)
    t.assert_equals(named, { 'event/order.named/1', 'event/order.named/2' })
    t.assert_equals(#listener.lanes, 2)
end

g.test_a_stopped_listener_refuses_what_is_offered_to_it_afterwards = function()
    local seen = {}
    local listener = listening(function(message)
        table.insert(seen, message.body)
    end)

    listener:stop()
    listener:stop()

    local placed, why = listener:offer(envelope_of('после остановки'), nil)

    t.assert_equals({ placed, why }, { false, 'слушатель order.paid остановлен' })
    t.assert_equals(
        listener:status().counts.dropped,
        1,
        'повторная остановка ничего не сосчитала'
    )
    t.assert_equals(given.withdrawn, { 'order.paid' }, 'снят с шины один раз')

    fiber.sleep(0.02)
    t.assert_equals(seen, {}, 'закрытая полоса событие не приняла')
end
