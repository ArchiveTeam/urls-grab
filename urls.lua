local urlparse = require("socket.url")
local http = require("socket.http")
JSON = (loadfile "JSON.lua")()

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
local bad_params = {}
local bad_patterns = {}

for param in io.open("bad-params", "r"):lines() do
  local param = string.gsub(
    param, "([a-zA-Z])",
    function(c)
      return "[" .. string.lower(c) .. string.upper(c) .. "]"
    end
  )
  table.insert(bad_params, param)
end

for pattern in io.open("bad-patterns", "r"):lines() do
  table.insert(bad_patterns, pattern)
end

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

remove_param = function(url, param_pattern)
  local newurl = url
  repeat
    url = newurl
    newurl = string.gsub(url, "([%?&;])" .. param_pattern .. "=[^%?&;]*[%?&;]?", "%1")
  until newurl == url
  return string.match(newurl, "^(.-)[%?&;]?$")
end

queue_new_urls = function(url)
  if string.match(url, "^https?://[^/]+/%(S%([a-z0-9A-Z]+%)%)") then
    return nil
  end
  local newurl = string.gsub(url, "([%?&;])amp;", "%1")
  if url == current_url then
    if newurl ~= url then
      queued_urls[newurl] = true
    end
  end
  for _, param_pattern in pairs(bad_params) do
    newurl = remove_param(newurl, param_pattern)
  end
  for s in string.gmatch(string.lower(newurl), "([a-f0-9]+)") do
    if string.len(s) == 32 then
      return nil
    end
  end
  if newurl ~= url then
    queued_urls[newurl] = true
  end
  newurl = string.match(newurl, "^([^%?&]+)")
  if newurl ~= url then
    queued_urls[newurl] = true
  end
end

report_bad_url = function(url)
  if current_url ~= nil then
    bad_urls[current_url] = true
  else
    bad_urls[string.lower(url)] = true
  end
end

strip_url = function(url)
  url = string.match(url, "^https?://(.+)$")
  newurl = string.match(url, "^www%.(.+)$")
  if newurl then
    url = newurl
  end
  return url
end

wget.callbacks.write_to_warc = function(url, http_stat)
  if bad_code(http_stat["statcode"]) then
    return false
  elseif http_stat["statcode"] ~= 200 then
    return true
  end
  if true then
    return true
  end
  if http_stat["len"] > 5 * 1024 * 1024 then
    io.stdout:write("Data larger than 5 MB. Checking with Wayback Machine.\n")
    io.stdout:flush()
    while true do
      local body, code, headers, status = http.request(
        "https://web.archive.org/__wb/calendarcaptures/2"
          .. "?url=" .. urlparse.escape(url["url"])
          .. "&date=20"
      )
      if code ~= 200 then
        io.stdout:write("Got " .. tostring(code) .. " from the Wayback Machine.\n")
        io.stdout:flush()
        os.execute("sleep 10")
      else
        data = JSON:decode(body)
        if not data["items"] or not data["colls"] then
          return true
        end
        for _, item in pairs(data["items"]) do
          if item[2] == 200 then
            local coll_id = item[3] + 1
            if not coll_id then
              io.stdout:write("Could get coll ID.\n")
              io.stdout:flush()
            end
            local collections = data["colls"][coll_id]
            if not collections then
              io.stdout:write("Could not get collections.\n")
              io.stdout:flush()
            end
            for _, collection in pairs(collections) do
              if collection == "archivebot"
                or string.find(collection, "archiveteam") then
                io.stdout:write("Archive Team got this URL before.\n")
                return false
              end
            end
          end
        end
        break
      end
    end
  end
  return true
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
    
    --[[if strip_url(url["url"]) == strip_url(newloc) then
      queued_urls[newloc] = true
      return wget.actions.EXIT
    end]]
    if string.match(newloc, "^https?://[^/]*google%.com/sorry") then
      report_bad_url(url["url"])
      return wget.actions.EXIT
    end
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
    report_bad_url(url["url"])
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
  local is_bad = false
  for url, _ in pairs(queued_urls) do
    for _, pattern in pairs(bad_patterns) do
      is_bad = string.match(url, pattern)
      if is_bad then
        break
      end
    end
    if not is_bad then
      io.stdout:write("Queuing URL " .. url .. ".\n")
      io.stdout:flush()
      if newurls == nil then
        newurls = url
      else
        newurls = newurls .. "\0" .. url
      end
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

