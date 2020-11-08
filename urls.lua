local urlparse = require("socket.url")
local http = require("socket.http")

local item_dir = os.getenv('item_dir')
local item_name = os.getenv("item_name")
local warc_file_base = os.getenv('warc_file_base')

local url_count = 0
local downloaded = {}
local abortgrab = false

if urlparse == nil or http == nil then
  io.stdout:write("socket not corrently installed.\n")
  io.stdout:flush()
  abortgrab = true
end

local urls = {}
for url in string.gmatch(item_name, "([^\n]+)") do
  urls[string.lower(url)] = true
end

local current_url = nil
local bad_urls = {}
local queued_urls = {}

bad_code = function(status_code)
  return status_code == 0
    or status_code == 401
    or status_code == 403
    or status_code == 407
    or status_code == 408
    or status_code == 411
    or status_code == 413
    or status_code == 429
    or status_code == 451
    or status_code >= 500
end

remove_param = function(url, param)
  url = string.gsub(url, "([%?&])" .. param .. "=[^%?&]*[%?&]?", "%1")
  return string.match(url, "^(.-)[%?&]?$")
end

queue_new_urls = function(url)
  local newurl = remove_param(url, "utm_source")
  newurl = remove_param(newurl, "utm_medium")
  newurl = remove_param(newurl, "utm_campaign")
  newurl = remove_param(newurl, "utm_term")
  newurl = remove_param(newurl, "utm_content")
  newurl = remove_param(newurl, "utm_adgroup")
  newurl = remove_param(newurl, "ref")
  newurl = remove_param(newurl, "refsrc")
  newurl = remove_param(newurl, "referrer_id")
  newurl = remove_param(newurl, "referrerid")
  newurl = remove_param(newurl, "src")
  newurl = remove_param(newurl, "i")
  newurl = remove_param(newurl, "s")
  newurl = remove_param(newurl, "ts")
  newurl = remove_param(newurl, "feature")
  if newurl ~= url then
    queued_urls[newurl] = true
  end
  newurl = string.match(url, "^([^%?&]+)")
  if newurl ~= url then
    queued_urls[newurl] = true
  end
  newurl = string.gsub(url, "([?&])amp;", "%1")
  if newurl ~= url then
    queued_urls[newurl] = true
  end
end

wget.callbacks.write_to_warc = function(url, http_stat)
  return not bad_code(http_stat["statcode"])
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]

  if urls[string.lower(url["url"])] then
    current_url = string.lower(url["url"])
  end
  
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. "  \n")
  io.stdout:flush()

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if downloaded[newloc] then
      return wget.actions.EXIT
    end
  end

  if downloaded[url["url"]] then
    return wget.actions.EXIT
  end
  
  if status_code >= 200 and status_code <= 399 then
    downloaded[url["url"]] = true
  end

  if status_code >= 200 and status_code < 300 then
    queue_new_urls(url["url"])
  end

  if bad_code(status_code) then
    io.stdout:write("Server returned " .. http_stat.statcode .. " (" .. err .. ").\n")
    io.stdout:flush()
    if current_url ~= nil then
      bad_urls[current_url] = true
    else
      bad_urls[string.lower(url["url"])] = true
    end
    return wget.actions.EXIT
  end

  local sleep_time = 0

  if sleep_time > 0.001 then
    os.execute("sleep " .. sleep_time)
  end

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local newurls = nil
  for url, _ in pairs(queued_urls) do
    if newurls == nil then
      newurls = url
    else
      newurls = newurls .. "\0" .. url
    end
  end

  if newurls ~= nil then
    local tries = 0
    while tries < 10 do
      local body, code, headers, status = http.request(
        "http://blackbird-amqp.meo.ws:23038/urls-m92hrwe0faimbhi/",
        newurls
      )
      if code == 200 or code == 409 then
        break
      end
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    if tries == 10 then
      abortgrab = true
    end
  end

  local file = io.open(item_dir .. '/' .. warc_file_base .. '_bad-urls.txt', 'w')
  for url, _ in pairs(bad_urls) do
    file:write(url .. "\n")
  end
  file:close()
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if abortgrab then
    return wget.exits.IO_FAIL
  end
  return exit_status
end

