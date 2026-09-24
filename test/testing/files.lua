--- Файлы целиком: прочитать и записать.
---
--- Проверкам файлы нужны на два дела — положить сценарий или конфигурацию
--- узлу и прочитать то, что он написал. Обёртки над `fio` в каждом
--- помощнике выходили одни и те же, вплоть до прав на файл.

local fio = require('fio')

local Module = {}

--- Права на создаваемый файл: 0644, читать могут все, писать — хозяин.
---
--- Десятичной записью: восьмеричных чисел у Lua нет, а `tonumber('644', 8)`
--- прячет число в строке.
Module.MODE = 420

--- Содержимое файла целиком.
---@param path string
---@return string
function Module.read(path)
    local file, err = fio.open(path, { 'O_RDONLY' })

    if file == nil then
        error(('файл %s не читается: %s'):format(path, tostring(err)))
    end

    local text = file:read()

    file:close()

    return text
end

--- Пишет файл целиком, затирая прежний.
---@param path string
---@param body string
function Module.write(path, body)
    local file, err = fio.open(path, { 'O_WRONLY', 'O_CREAT', 'O_TRUNC' }, Module.MODE)

    if file == nil then
        error(('файл %s не записывается: %s'):format(path, tostring(err)))
    end

    file:write(body)
    file:close()
end

return Module
