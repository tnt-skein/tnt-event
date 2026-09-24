--- Шина в транзакции настоящего `box`: событие уходит после фиксации,
--- при откате не уходит вовсе.
---
--- В процессе проверок `box` не настроен, и ветка транзакции идёт там
--- двойником внешней зависимости. Здесь — настоящие `box.is_in_txn`
--- и `box.on_commit`: укладка из триггера обязана обойтись без уступки,
--- иначе процесс падает сигналом, и увидеть это можно только на узле.

local t = require('luatest')

local helper = dofile('test/helper.lua')

local g = t.group('tnt.event.txn')

g.before_all(function()
    g.server = helper.start_node()
end)

g.after_all(function()
    helper.stop_node(g.server)
end)

g.test_an_event_dispatched_in_a_transaction_goes_after_the_commit = function()
    local seen = g.server:exec(function()
        local fiber = require('fiber')
        local context = require('tnt.context')
        local event = require('tnt.event')

        local orders = box.schema.space.create('orders')

        orders:create_index('primary', {})

        local handled = {}

        event.listen('order.paid', function(message)
            table.insert(handled, {
                body = message.body,
                request_id = context.get('request_id'),
                in_txn = box.is_in_txn(),
            })
        end)

        -- Откат: ни строки, ни события.
        box.begin()
        orders:insert({ 1 })
        event.dispatch('order.paid', { order = 1 })
        box.rollback()
        fiber.sleep(0.05)

        local after_rollback = #handled

        -- Фиксация: строка и событие, опознаватель запроса — из мига вызова.
        local sent = context.run({ request_id = 'r-9' }, function()
            return box.atomic(function()
                orders:insert({ 2 })

                return event.dispatch('order.paid', { order = 2 })
            end)
        end)

        t.helpers.retrying({ timeout = 5, delay = 0.01 }, function()
            assert(#handled == 1, 'событие после фиксации не пришло')
        end)

        -- Вне транзакции событие уходит в миг вызова.
        event.dispatch('order.paid', { order = 3 })
        t.helpers.retrying({ timeout = 5, delay = 0.01 }, function()
            assert(#handled == 2, 'событие вне транзакции не пришло')
        end)

        local status = event.status()

        event.reset()

        return {
            sent = sent,
            after_rollback = after_rollback,
            handled = handled,
            orders = orders:count(),
            status = status,
        }
    end)

    t.assert_equals(seen.after_rollback, 0, 'при откате события нет')
    t.assert_equals(
        seen.orders,
        1,
        'строка 1 откатилась вместе с событием, осталась только 2'
    )
    t.assert_equals(seen.handled[1].body, { order = 2 })
    t.assert_equals(
        seen.handled[1].request_id,
        'r-9',
        'контекст вызывающего доехал заголовком'
    )
    t.assert_equals(
        seen.handled[1].in_txn,
        false,
        'обработчик идёт своим файбером, вне транзакции'
    )
    t.assert_equals(seen.handled[2].body, { order = 3 })
    t.assert_equals(#seen.sent, 26)
    t.assert_equals(seen.status.counts.sent, 2, 'откаченное отправленным не считается')
end

g.test_a_refusal_inside_the_commit_trigger_does_not_kill_the_process = function()
    local seen = g.server:exec(function()
        local fiber = require('fiber')
        local event = require('tnt.event')

        local busy = fiber.cond()
        local taken = {}

        event.listen('order.slow', function(message)
            table.insert(taken, message.body)
            busy:wait(1)
        end, { capacity = 1 })

        -- Три события одной транзакцией: место в полосе одно, и после
        -- фиксации двум из трёх его не хватит. Отказ отдать некому —
        -- он уходит записью, а запись идёт из триггера фиксации, где
        -- уступка убила бы процесс сигналом.
        box.atomic(function()
            for number = 1, 3 do
                event.dispatch('order.slow', number)
            end
        end)

        t.helpers.retrying({ timeout = 5, delay = 0.01 }, function()
            assert(#taken == 1, 'первое событие не взято')
        end)

        local status = event.status()

        busy:broadcast()
        event.reset()

        return { taken = taken, counts = status.counts, alive = box.info.status }
    end)

    t.assert_equals(
        seen.alive,
        'running',
        'узел жив: укладка из триггера не уступала'
    )
    t.assert_equals(seen.taken, { 1 }, 'первое событие дошло до обработчика')
    t.assert_equals(seen.counts.sent, 1)
    t.assert_equals(seen.counts.refused, 2, 'двум событиям места не нашлось')
end
