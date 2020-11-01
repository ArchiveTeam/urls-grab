local urlparse = require("socket.url")

local item_dir = os.getenv('item_dir')
local item_name = os.getenv("item_name")
local warc_file_base = os.getenv('warc_file_base')

local url_count = 0
local tries = 0
local downloaded = {}

local urls = {}
for url in string.gmatch(item_name, "([^\n]+)") do
  urls[string.lower(url)] = true
end

local current_url = nil
local bad_urls = {}

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

wget.callbacks.write_to_warc = function(url, http_stat)
  return not bad_code(http_stat["statcode"])
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]

  if urls[string.lower(url["url"])] then
    current_url = url["url"]
  end
  
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. "  \n")
  io.stdout:flush()

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if downloaded[newloc] then
      tries = 0
      return wget.actions.EXIT
    end
  end

  if downloaded[url["url"]] then
    return wget.actions.EXIT
  end
  
  if status_code >= 200 and status_code <= 399 then
    downloaded[url["url"]] = true
  end

  if bad_code(status_code) then
    io.stdout:write("Server returned " .. http_stat.statcode .. " (" .. err .. "). Sleeping.\n")
    io.stdout:flush()
    if tries >= 3 then
      io.stdout:write("Skipping URL...\n")
      io.stdout:flush()
      tries = 0
      if current_urls then
        bad_urls[current_url] = true
      else
        bad_urls[url["url"]] = true
      end
      return wget.actions.EXIT
    else
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
      return wget.actions.CONTINUE
    end
  end

  tries = 0

  local sleep_time = 0

  if sleep_time > 0.001 then
    os.execute("sleep " .. sleep_time)
  end

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local file = io.open(item_dir .. '/' .. warc_file_base .. '_bad-urls.txt', 'w')
  for url, _ in pairs(bad_urls) do
    file:write(url .. "\n")
  end
  file:close()
end

