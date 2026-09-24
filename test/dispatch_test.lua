--- Настройки отправки шины: чего шина не умеет и почему.
---
--- Сами настройки, тело и конверт проверяет общая часть договора; здесь —
--- то, что у шины своё: признаки и причины отказа.

local t = require('luatest')

local helper = dofile('test/helper.lua')

local event = helper.event

local g = t.group('tnt.event.dispatch')

g.before_each(helper.forget)

g.after_all(function()
    helper.journal.release()
end)

g.test_settings_the_bus_cannot_do_are_named_one_by_one = function()
    helper.assert_blamed({
        {
            function()
                event.dispatch('order.paid', 'тело', { priority = 0 })
            end,
            'настройки отправки: «priority» — шина событий не умеет: '
                .. 'очередь слушателя выдаёт по порядку прихода',
        },
        {
            -- Неумение называется раньше негодного значения: переехавший
            -- код правят не подгонкой аргумента, а переносом на очередь,
            -- которая срок жизни умеет.
            function()
                event.dispatch('order.paid', 'тело', { ttl = helper.wrong('навсегда') })
            end,
            'настройки отправки: «ttl» — шина событий не умеет: событие ждёт слушателя, '
                .. 'пока жив процесс, и сроку жизни неоткуда взяться',
        },
    })
end

g.test_what_the_bus_can_do_passes = function()
    local sent, err = event.dispatch('order.paid', 'тело', { key = 'a', delay = 0, id = 'x', timeout = 3 })

    t.assert_equals({ sent, err }, { 'x', nil })
end

g.test_a_body_deeper_than_the_codecs_take_is_raised_at_the_caller = function()
    -- Шине в памяти годилась бы и такая глубина, но обработчик шины обязан
    -- без правки переехать на очередь, а там кодек тело не запишет.
    helper.assert_blamed({
        {
            function()
                event.dispatch('order.paid', helper.deep(126))
            end,
            'тело — вложенность глубже 100 таблиц',
        },
    })
end
