--- Шина: объявление слушателей, рассылка, сводка, зарытые и сброс.
---
--- Транзакция вызывающего приходит как внешняя зависимость: здесь проверяется, что конверт
--- собран в миг вызова, а укладка идёт задачей фиксации и отказ полной
--- очереди после неё некому отдать. Тот же путь на настоящем `box` —
--- в `txn_node_test.lua`.

local fiber = require('fiber')
local t = require('luatest')

local helper = dofile('test/helper.lua')

local context = helper.context
local event = helper.event

local g = t.group('tnt.event')

g.before_each(function()
    helper.forget()
end)

g.after_all(function()
    helper.forget()
    helper.journal.release()
end)

--- Слушатель, который складывает конверты в список.
---@param seen table[]
---@param opts table|nil
---@return TntEventListener
local function collecting(seen, opts)
    return event.listen('order.paid', function(envelope)
        table.insert(seen, envelope)
    end, opts)
end

--- Задачи фиксации, поставленные шиной под подменённой внешней зависимостью.
---@return fun()[] tasks
local function in_transaction()
    local tasks = {}

    event._set_source({
        in_txn = function()
            return true
        end,
        on_commit = function(task)
            table.insert(tasks, task)
        end,
    })

    return tasks
end

g.test_the_bus_says_what_it_can_do = function()
    t.assert_equals(event.features, {
        delay = true,
        ttl = false,
        ttr = false,
        touch = false,
        priority = false,
        key = true,
        transactional = true,
        atomic = false,
    })
end

g.test_an_event_nobody_listens_to_is_not_a_refusal = function()
    local sent, err = event.dispatch('order.paid', { order = 11 })

    t.assert_equals(#sent, 26, 'опознаватель — ULID')
    t.assert_equals(err, nil, 'шина не знает, кому событие было нужно')
    t.assert_equals(event.status().counts.sent, 1)
end

g.test_every_listener_of_the_name_gets_its_own_copy = function()
    local first, second, other = {}, {}, {}

    collecting(first)
    collecting(second)
    event.listen('order.shipped', function(envelope)
        table.insert(other, envelope)
    end)

    local sent = context.run({ request_id = 'r-7' }, function()
        return event.dispatch('order.paid', { order = 11 }, { key = 'o-11' })
    end)

    helper.until_true('получили не оба слушателя', function()
        return #first == 1 and #second == 1
    end)

    first[1].body.order = 'подменено'

    t.assert_equals(second[1].body, { order = 11 }, 'правка конверта соседа не задела')
    t.assert_equals(second[1].id, sent)
    t.assert_equals(second[1].key, 'o-11')
    t.assert_equals(second[1].headers, { ['x-request-id'] = 'r-7' })
    t.assert_equals(other, {}, 'событие другого имени не пришло')
end

g.test_a_name_with_a_dot_and_a_dash_is_a_name = function()
    local seen = {}

    event.listen('order.paid-again_2', function(envelope)
        table.insert(seen, envelope.body)
    end)
    event.dispatch('order.paid-again_2', 'ещё раз')

    helper.until_true('событие не пришло', function()
        return #seen == 1
    end)
end

g.test_a_very_long_name_does_not_kill_the_worker = function()
    local seen = {}
    -- Имя длиннее предела имени файбера (255 байт): работник обрезает его
    -- себе, а не умирает на первом же событии.
    local name = 'order.' .. ('name'):rep(80)

    event.listen(name, function(envelope)
        table.insert(seen, envelope.body)
    end)
    event.dispatch(name, 'длинное имя')

    helper.until_true('работник с длинным именем не взял событие', function()
        return #seen == 1
    end)
end

g.test_wrong_arguments_are_raised_at_the_caller = function()
    helper.assert_blamed({
        {
            function()
                event.listen('order paid', print)
            end,
            'имя события — строка по образцу ^%a[%w_.-]*$, а не «order paid»',
        },
        {
            function()
                event.listen('order.paid', helper.wrong('не функция'))
            end,
            'обработчик — функция или вызываемая таблица, а не строка',
        },
        {
            function()
                event.listen('order.paid', print, { atomic = true })
            end,
            'настройки слушателя: «atomic» — шина событий не умеет: обработчик идёт своим файбером '
                .. 'после фиксации, и транзакции вызывающего там уже нет',
        },
        {
            function()
                event.dispatch('7 заказов', 'тело')
            end,
            'имя события — строка по образцу ^%a[%w_.-]*$, а не «7 заказов»',
        },
        {
            function()
                event.dispatch('order.paid', 'тело', { ttl = 5 })
            end,
            'настройки отправки: «ttl» — шина событий не умеет: событие ждёт слушателя, '
                .. 'пока жив процесс, и сроку жизни неоткуда взяться',
        },
        {
            function()
                event.dispatch('order.paid', { at = print })
            end,
            'тело.at — простые данные, а не function',
        },
    })
end

g.test_a_full_listener_refuses_and_does_not_rob_the_others = function()
    local busy = fiber.cond()
    local slow, fast = {}, {}

    event.listen('order.paid', function(envelope)
        table.insert(slow, envelope.body)
        busy:wait(1)
    end, { capacity = 1 })
    collecting(fast)

    event.dispatch('order.paid', 'в работе')
    helper.until_true('первое не взято', function()
        return #slow == 1
    end)

    event.dispatch('order.paid', 'займёт очередь')

    local sent, err = event.dispatch('order.paid', 'лишнее')

    t.assert_equals(sent, nil)
    t.assert_equals(err.kind, 'overflow')
    t.assert_equals(err.sent, false, 'событие не ушло')
    t.assert_equals(
        err.retriable,
        false,
        'ждать места незачем: место освободит слушатель'
    )
    t.assert_equals(
        tostring(err),
        'очередь слушателя order.paid полна: 1 событий в полосе'
    )
    t.assert_equals(event.status().counts, { sent = 2, refused = 1, evicted = 0 })

    helper.until_true('быстрый слушатель не получил все три', function()
        return #fast == 3
    end)

    busy:broadcast()
end

g.test_the_status_shows_the_listeners_by_name = function()
    local seen = {}

    collecting(seen, { workers = 2 })
    collecting(seen)
    event.listen('order.shipped', print)

    event.hook('tnt.trace', function(_, proceed)
        return proceed()
    end)

    local status = event.status()

    t.assert_equals(status.counts, { sent = 0, refused = 0, evicted = 0 })
    t.assert_equals(status.dead, 0)
    t.assert_equals(status.hooks, { 'tnt.trace' })
    t.assert_equals(
        #status.listeners['order.paid'],
        2,
        'у имени бывает несколько слушателей'
    )
    t.assert_equals(status.listeners['order.paid'][1], {
        name = 'order.paid',
        workers = 2,
        capacity = 1000,
        depth = 0,
        taken = 0,
        delayed = 0,
        counts = { ack = 0, retry = 0, bury = 0, dropped = 0, failures = 0 },
    })
    t.assert_equals(status.listeners['order.shipped'][1].workers, 1, 'умолчание — один работник')
end

g.test_a_stopped_listener_leaves_the_bus_and_its_neighbour_stays = function()
    local first, second = {}, {}
    local leaving = collecting(first)

    collecting(second)
    leaving:stop()
    event.dispatch('order.paid', 'после остановки')

    helper.until_true('сосед не получил событие', function()
        return #second == 1
    end)

    t.assert_equals(first, {}, 'снятому слушателю событие не идёт')
    t.assert_equals(#event.status().listeners['order.paid'], 1)

    second[1] = nil
    event.reset()
    event.dispatch('order.paid', 'после сброса')
    fiber.sleep(0.01)

    t.assert_equals(second, {}, 'сброс снял и второго')
    t.assert_equals(event.status().listeners, {}, 'имён без слушателей в сводке нет')

    -- Остановка снятого слушателя безвредна: сброс уже забыл его имя.
    leaving:stop()
end

g.test_the_buried_are_remembered_up_to_a_hundred = function()
    event.listen('order.paid', function(envelope)
        return nil, { retriable = false, message = 'негодное ' .. envelope.body }
    end)

    for number = 1, 101 do
        event.dispatch('order.paid', number)
    end

    helper.until_true('зарыты не все', function()
        return event.status().counts.evicted == 1
    end)

    local dead = event.dead()

    t.assert_equals(#dead, 100, 'потолок списка зарытых')
    t.assert_equals(event.status().dead, 100)
    t.assert_equals(dead[1].reason, 'негодное 2', 'самое старое вытеснено')
    t.assert_equals(dead[100].reason, 'негодное 101')
    t.assert_equals(dead[1].name, 'order.paid')
    t.assert_equals(dead[1].attempt, 1)
    t.assert_almost_equals(dead[1].buried, require('clock').realtime(), 5)
    t.assert_equals(#dead[1].id, 26)

    dead[1].reason = 'подменено'

    t.assert_equals(event.dead()[1].reason, 'негодное 2', 'список отдаётся копией')

    event.reset()
    t.assert_equals(event.dead(), {}, 'сброс забыл зарытых')
    t.assert_equals(event.status().counts, { sent = 0, refused = 0, evicted = 0 })
end

g.test_a_hook_sees_both_the_sending_and_the_handling = function()
    local seen = {}

    event.hook('tnt.trace', function(call, proceed)
        local first, second, third = proceed()

        table.insert(seen, {
            kind = call.kind,
            name = call.name,
            body = call.message and call.message.body,
            first = first,
            second = second,
            third = third,
        })

        return nil
    end)

    local sent = event.dispatch('order.paid', 'без слушателей')

    t.assert_equals(seen[1].kind, 'send')
    t.assert_equals(seen[1].name, 'order.paid')
    t.assert_equals(seen[1].body, 'без слушателей', 'конверт появился внутри proceed')
    t.assert_equals(seen[1].first, sent)

    event.listen('order.paid', function()
        return nil, { retriable = false, message = 'зарыть' }
    end)
    event.dispatch('order.paid', 'для обработчика')

    helper.until_true('обработка не прошла через крюк', function()
        return #seen == 3
    end)

    t.assert_equals(
        seen[2].kind,
        'send',
        'отправка записана до обработки: укладка не уступает'
    )
    t.assert_equals(seen[3].kind, 'handle', 'обработка тоже под крюком')
    t.assert_equals(seen[3].body, 'для обработчика')
    t.assert_equals(seen[3].first, true, 'обработчик дошёл до возврата')
    t.assert_equals(seen[3].third.message, 'зарыть')
end

g.test_a_delay_is_passed_to_the_listener = function()
    local seen = {}

    collecting(seen)
    event.dispatch('order.paid', 'подождёт', { delay = 60 })
    fiber.sleep(0.02)

    t.assert_equals(seen, {}, 'отсроченное событие обработчику не пошло')
    t.assert_equals(event.status().listeners['order.paid'][1].depth, 0, 'оно у работника')
end

g.test_in_a_transaction_the_event_goes_after_the_commit = function()
    local seen = {}
    local tasks = in_transaction()

    collecting(seen)

    local sent = context.run({ request_id = 'r-9' }, function()
        return event.dispatch('order.paid', 'в транзакции')
    end)

    t.assert_equals(#sent, 26, 'опознаватель приходит сразу')
    t.assert_equals(#tasks, 1, 'укладка отложена до фиксации')
    t.assert_equals(seen, {}, 'до фиксации события нет')

    assert(tasks[1], 'задачи фиксации нет')()
    helper.until_true('после фиксации событие не пришло', function()
        return #seen == 1
    end)

    t.assert_equals(seen[1].id, sent)
    t.assert_equals(
        seen[1].headers,
        { ['x-request-id'] = 'r-9' },
        'контекст взят в миг вызова, а не в фиксации'
    )
    t.assert_equals(
        event.status().counts.sent,
        1,
        'отправленным событие стало после фиксации'
    )
end

g.test_a_rolled_back_transaction_leaves_no_event = function()
    local seen = {}
    local tasks = in_transaction()

    collecting(seen)
    event.dispatch('order.paid', 'откатится')
    fiber.sleep(0.01)

    t.assert_equals(#tasks, 1, 'задача фиксации так и не позвана')
    t.assert_equals(seen, {}, 'при откате события нет')
    t.assert_equals(event.status().counts.sent, 0)
end

g.test_after_the_commit_a_refusal_goes_to_the_journal = function()
    local busy = fiber.cond()
    local seen = {}
    local tasks = in_transaction()

    event.listen('order.paid', function(envelope)
        table.insert(seen, envelope.body)
        busy:wait(1)
    end, { capacity = 1 })

    for number = 1, 3 do
        event.dispatch('order.paid', number)
    end

    assert(tasks[1], 'задачи фиксации нет')()
    helper.until_true('первое не взято', function()
        return #seen == 1
    end)

    -- Второе занимает освободившуюся полосу, третьему места уже нет,
    -- и отказать после фиксации некому: вызывающий давно ушёл.
    assert(tasks[2], 'второй задачи фиксации нет')()
    assert(tasks[3], 'третьей задачи фиксации нет')()

    local record = helper.journal.find('событие после фиксации не поместилось').record

    t.assert_equals(record.fields.destination, 'order.paid')
    t.assert_str_contains(record.fields.err, 'очередь слушателя order.paid полна')
    t.assert_equals(event.status().counts, { sent = 2, refused = 1, evicted = 0 })

    busy:broadcast()
end
