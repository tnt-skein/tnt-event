--- Настройки слушателя: общие настройки получателя и свой потолок очереди.

local t = require('luatest')

local helper = dofile('test/helper.lua')

local event = helper.event
local listener = helper.listener

local g = t.group('tnt.event.policy')

--- Настройки слушателя у этой шины.
---@param opts table|nil
---@return TntEventPolicy
local function policy(opts)
    return listener.policy(opts, event.features, 1)
end

g.test_the_policy_has_defaults_for_everything = function()
    t.assert_equals(policy(nil), {
        workers = 1,
        max_attempts = 5,
        capacity = 1000,
        backoff = { base = 1, max = 60 },
        atomic = false,
    })
    t.assert_equals(listener.CAPACITY, 1000)
end

g.test_the_policy_takes_what_is_given = function()
    local given = { workers = 4, max_attempts = 2, capacity = 8, backoff = { base = 0.1, max = 1 } }
    local expected = table.deepcopy(given)

    expected.atomic = false

    t.assert_equals(policy(given), expected)
end

g.test_atomic_is_known_but_impossible = function()
    local impossible = 'настройки слушателя: «atomic» — шина событий не умеет: обработчик идёт своим файбером '
        .. 'после фиксации, и транзакции вызывающего там уже нет'

    helper.assert_blamed({
        {
            function()
                policy({ atomic = true })
            end,
            impossible,
        },
        {
            -- И `atomic = false` — исключение: настройка, которой нет,
            -- не значит «выключено», иначе переезд на шину прошёл бы молча.
            function()
                policy({ atomic = false })
            end,
            impossible,
        },
    })
end

g.test_wrong_policy_settings_are_raised_at_the_caller = function()
    helper.assert_blamed({
        {
            function()
                policy({ worker = 2 })
            end,
            'настройки слушателя: ключа «worker» нет, есть atomic, backoff, capacity, max_attempts, workers',
        },
        {
            function()
                policy({ workers = 0 })
            end,
            'настройки слушателя.workers — число больше 0, а не 0',
        },
        {
            function()
                policy({ capacity = 0 })
            end,
            'настройки слушателя.capacity — число больше 0, а не 0',
        },
        {
            function()
                policy({ capacity = 1.5 })
            end,
            'настройки слушателя.capacity — целое число, а не 1.5',
        },
    })
end

g.test_wrong_settings_of_listen_blame_its_caller = function()
    helper.assert_blamed({
        {
            function()
                event.listen('order.paid', print, { capacity = 0 })
            end,
            'настройки слушателя.capacity — число больше 0, а не 0',
        },
    })
end
