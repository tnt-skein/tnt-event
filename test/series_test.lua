--- Ряды шины: разослано и отказано, итоги слушателей, ожидание первой
--- выдачи и глубина по состояниям — так, как их видит сборщик.
---
--- Роды рядов, их метки и сложение глубины проверяет общая часть договора;
--- здесь — что настоящая работа шины до рядов доходит и что глубина
--- берётся из полос, работников и зарытых. Имена событий у каждой проверки
--- свои: реестр общий на процесс, и числа соседней проверки остаются в нём.

local fiber = require('fiber')
local t = require('luatest')

local helper = dofile('test/helper.lua')

local event = helper.event

local g = t.group('tnt.event.series')

--- Обработчики, которые ждут отмашки проверки: их отпускает `after_each`,
--- иначе работник остался бы висеть до конца прогона.
local gates = {}

g.before_each(function()
    gates = {}
    helper.forget()
end)

g.after_each(function()
    for _, gate in ipairs(gates) do
        gate:broadcast()
    end

    helper.forget()
end)

g.after_all(function()
    helper.journal.release()
end)

--- Число ряда назначения — после наблюдения этой загрузки: проверки
--- соседних файлов кладут в реестр ряды своей загрузки, и наши возвращает
--- туда первое же наблюдение.
---@param name string Имя ряда
---@param labels table Метки, кроме `destination`
---@param destination string
---@return number|nil
local function row(name, labels, destination)
    helper.series.count({ sent = 0 }, 'series.probe', 'sent')
    labels.destination = destination

    return helper.value(name, labels)
end

--- Глубина события по состояниям; пустая таблица — строк нет.
---@param destination string
---@return table<string, number>
local function depth_of(destination)
    helper.series.count({ sent = 0 }, 'series.probe', 'sent')

    local found = {}

    for _, sample in ipairs(helper.samples('message_depth')) do
        if sample.label_pairs.destination == destination then
            found[sample.label_pairs.state] = sample.value
        end
    end

    return found
end

--- Обработчик, который держит событие, пока проверка не отпустит.
---@param seen table[] Куда класть тела
---@return fun(message: TntMessage)
local function held(seen)
    local gate = fiber.cond()

    table.insert(gates, gate)

    return function(message)
        table.insert(seen, message.body)
        gate:wait()
    end
end

g.test_a_dispatch_counts_what_went_and_what_overflowed = function()
    local seen = {}

    event.listen('series.full', held(seen), { capacity = 1 })

    t.assert_equals(event.dispatch('series.full', 'первое') ~= nil, true)
    helper.until_true('работник не взял первое', function()
        return #seen == 1
    end)
    t.assert_equals(event.dispatch('series.full', 'второе') ~= nil, true)

    local sent, err = event.dispatch('series.full', 'третье')

    t.assert_equals(sent, nil)
    t.assert_equals(err.kind, 'overflow')
    t.assert_equals(event.status().counts, { sent = 2, refused = 1, evicted = 0 })
    t.assert_equals(row('message_sent_total', {}, 'series.full'), 2)
    t.assert_equals(row('message_send_failures_total', { kind = 'overflow' }, 'series.full'), 1)
    t.assert_equals(
        depth_of('series.full'),
        { ready = 1, taken = 1, delayed = 0, dead = 0 },
        'одно у работника, одно ждёт в полосе'
    )
end

g.test_the_outcomes_of_a_listener_grow_the_handled_rows = function()
    local unreadable = setmetatable({}, {
        __tostring = function()
            error('причина не читается', 0)
        end,
    })

    event.listen('series.outcomes', function(message)
        if message.body == 'негодное' then
            return nil, { retriable = false, message = 'тело не прошло проверку' }
        end

        if message.body == 'битое' then
            return nil, unreadable
        end
    end, { max_attempts = 1 })

    for _, body in ipairs({ 'сразу', 'негодное', 'битое' }) do
        event.dispatch('series.outcomes', body)
    end

    helper.until_true('события не разобраны', function()
        local counts = event.status().listeners['series.outcomes'][1].counts

        return counts.ack == 1 and counts.bury == 1 and counts.failures == 1
    end)

    t.assert_equals(row('message_handled_total', { outcome = 'ack' }, 'series.outcomes'), 1)
    t.assert_equals(row('message_handled_total', { outcome = 'bury' }, 'series.outcomes'), 1)
    t.assert_equals(row('message_worker_failures_total', {}, 'series.outcomes'), 1)
    t.assert_equals(
        depth_of('series.outcomes'),
        { ready = 0, taken = 0, delayed = 0, dead = 1 },
        'сорвавшийся шаг работника в обработке не числится'
    )
end

g.test_a_retry_is_counted_and_the_wait_is_measured_once_after_the_delay = function()
    local attempts = 0

    event.listen('series.retry', function()
        attempts = attempts + 1

        if attempts == 1 then
            return nil, 'служба молчит'
        end
    end, { backoff = { base = 0.01, max = 0.01 } })

    event.dispatch('series.retry', 'работа', { delay = 0.2 })
    helper.until_true('событие не подтверждено', function()
        return event.status().listeners['series.retry'][1].counts.ack == 1
    end)

    t.assert_equals(row('message_handled_total', { outcome = 'retry' }, 'series.retry'), 1)
    t.assert_equals(row('message_handled_total', { outcome = 'ack' }, 'series.retry'), 1)
    t.assert_equals(
        row('message_wait_seconds_count', {}, 'series.retry'),
        1,
        'повторная выдача в ожидание не идёт'
    )
    -- Отсрочку отмеряет время цикла событий, а отметку отправки ставят
    -- настоящие часы после работы без уступки: ожидание выходит короче
    -- отсрочки на эту работу. Без отсрочки оно было бы около нуля.
    t.assert_ge(
        row('message_wait_seconds_sum', {}, 'series.retry'),
        0.15,
        'отсрочка — часть ожидания'
    )
end

g.test_the_listeners_of_one_name_add_up_in_the_depth = function()
    local first, second = {}, {}

    event.listen('series.shared', held(first))
    event.listen('series.shared', held(second))

    for _, body in ipairs({ 'раз', 'два', 'три' }) do
        event.dispatch('series.shared', body)
    end

    helper.until_true('работники не взяли по событию', function()
        return #first == 1 and #second == 1
    end)

    local shown = event.status().listeners['series.shared'][1]

    t.assert_equals({ shown.depth, shown.taken, shown.delayed }, { 2, 1, 0 })
    t.assert_equals(
        depth_of('series.shared'),
        { ready = 4, taken = 2, delayed = 0, dead = 0 },
        'у каждого слушателя своя копия'
    )
end

g.test_an_event_asleep_before_its_first_delivery_or_its_repeat_is_delayed = function()
    local attempts = 0

    event.listen('series.asleep', function()
        attempts = attempts + 1

        return nil, 'служба молчит'
    end, { backoff = { base = 60, max = 60 } })

    event.dispatch('series.asleep', 'повторяется')
    event.dispatch('series.asleep', 'позже', { delay = 60 })

    helper.until_true('первая выдача не случилась', function()
        return attempts == 1
    end)

    t.assert_equals(depth_of('series.asleep'), { ready = 1, taken = 0, delayed = 1, dead = 0 })

    event.listen('series.later', function() end, { workers = 2 })
    event.dispatch('series.later', 'позже', { delay = 60, key = 'a' })
    event.dispatch('series.later', 'ещё позже', { delay = 60, key = 'c' })
    fiber.sleep(0.01)

    t.assert_equals(depth_of('series.later'), { ready = 0, taken = 0, delayed = 2, dead = 0 })
    t.assert_equals(event.status().listeners['series.later'][1].delayed, 2)
end

g.test_the_buried_stay_in_the_depth_after_their_listener_stops = function()
    local listener = event.listen('series.buried', function()
        return nil, { retriable = false, message = 'негодное' }
    end)

    event.dispatch('series.buried', 'раз')
    event.dispatch('series.buried', 'два')
    helper.until_true('события не зарыты', function()
        return event.status().dead == 2
    end)

    event.listen('series.gone', function() end):stop()
    listener:stop()

    t.assert_equals(
        depth_of('series.buried'),
        { ready = 0, taken = 0, delayed = 0, dead = 2 },
        'зарытым нужен человек, а не слушатель'
    )
    t.assert_equals(
        depth_of('series.gone'),
        {},
        'имени без слушателей и зарытых в ряду нет'
    )
end
