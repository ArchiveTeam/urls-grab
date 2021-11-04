local urlparse = require("socket.url")
local http = require("socket.http")
JSON = (loadfile "JSON.lua")()

local item_dir = os.getenv("item_dir")
local item_name = os.getenv("item_name")
local custom_items = os.getenv("custom_items")
local warc_file_base = os.getenv("warc_file_base")

local url_count = 0
local downloaded = {}
local abortgrab = false
local exit_url = false

if urlparse == nil or http == nil then
  io.stdout:write("socket not corrently installed.\n")
  io.stdout:flush()
  abortgrab = true
end

local urls = {}
for url in string.gmatch(item_name, "([^\n]+)") do
  urls[string.lower(url)] = true
end

local urls_settings = JSON:decode(custom_items)
for k, _ in pairs(urls_settings) do
  urls[string.lower(k)] = true
end

local status_code = nil

local redirect_urls = {}
local visited_urls = {}

local ids_to_ignore = {}
local uuid = ""
for _, i in pairs({8, 4, 4, 4, 12}) do
  for j=1,i do
    uuid = uuid .. "[0-9a-fA-F]"
  end
  if i ~= 12 then
    uuid = uuid .. "%-"
  end
end
ids_to_ignore[uuid] = true
local to_ignore = ""
for i=1,9 do
  to_ignore = to_ignore .. "[0-9]"
end
ids_to_ignore["%?" .. to_ignore .. "$"] = true
ids_to_ignore["%?" .. to_ignore .. "[0-9]$"] = true
ids_to_ignore[to_ignore .. "[0-9]%.[0-9][0-9][0-9][0-9]$"] = true
to_ignore = ""
for i=1,50 do
  to_ignore = to_ignore .. "[0-9a-zA-Z]"
end
ids_to_ignore[to_ignore .. "%-[0-9][0-9][0-9][0-9][0-9]"] = true
ids_to_ignore["[0-9a-zA-Z%-_]!%-?[0-9]"] = true
to_ignore = ""
for i=1,32 do
  to_ignore = to_ignore .. "[0-9a-fA-F]"
end
ids_to_ignore["[^0-9a-fA-F]" .. to_ignore .. "[^0-9a-fA-F]"] = true
ids_to_ignore["[^0-9a-fA-F]" .. to_ignore .. "$"] = true

local current_url = nil
local current_settings = nil
local bad_urls = {}
local queued_urls = {}
local bad_params = {}
local bad_patterns = {}
local page_requisite_patterns = {}
local duplicate_urls = {}
local extract_outlinks_patterns = {}
local item_first_url = nil
local redirect_domains = {}
local checked_domains = {}

local parenturl_uuid = nil
local parenturl_requisite = nil

local dupes_file = io.open("duplicate-urls.txt", "r")
for url in dupes_file:lines() do
  duplicate_urls[url] = true
end
dupes_file:close()

local bad_params_file = io.open("bad-params.txt", "r")
for param in bad_params_file:lines() do
  local param = string.gsub(
    param, "([a-zA-Z])",
    function(c)
      return "[" .. string.lower(c) .. string.upper(c) .. "]"
    end
  )
  table.insert(bad_params, param)
end
bad_params_file:close()

local bad_patterns_file = io.open("bad-patterns.txt", "r")
for pattern in bad_patterns_file:lines() do
  table.insert(bad_patterns, pattern)
end
bad_patterns_file:close()

local page_requisite_patterns_file = io.open("page-requisite-patterns.txt", "r")
for pattern in page_requisite_patterns_file:lines() do
  table.insert(page_requisite_patterns, pattern)
end
page_requisite_patterns_file:close()

local extract_outlinks_patterns_file = io.open("extract-outlinks-patterns.txt", "r")
for pattern in extract_outlinks_patterns_file:lines() do
  extract_outlinks_patterns[pattern] = true
end
extract_outlinks_patterns_file:close()

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

check_domain_outlinks = function(url, target)
  local parent = string.match(url, "^https?://([^/]+)")
  while parent do
    if (not target and extract_outlinks_patterns[parent])
      or (target and parent == target) then
      return parent
    end
    parent = string.match(parent, "^[^%.]+%.(.+)$")
  end
  return false
end

bad_code = function(status_code)
  return status_code == 0
    or status_code == 400
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

find_path_loop = function(url, max_repetitions)
  local tested = {}
  for s in string.gmatch(url, "([^/]+)") do
    s = string.lower(s)
    if not tested[s] then
      if s == "" then
        tested[s] = -1
      else
        tested[s] = 0
      end
    end
    tested[s] = tested[s] + 1
    if tested[s] == max_repetitions then
      return true
    end
  end
  return false
end

queue_url = function(url, withcustom)
--local original = url
  load_setting_depth = function(s)
    n = tonumber(current_settings[s])
    if n == nil then
      n = 0
    end
    return n - 1
  end
  temp = ""
  for c in string.gmatch(url, "(.)") do
    local b = string.byte(c)
    if b < 32 or b > 126 then
      c = string.format("%%%02X", b)
    end
    temp = temp .. c
  end
  url = temp
  if current_settings and current_settings["all"] and withcustom then
    local depth = load_setting_depth("depth")
    local keep_random = load_setting_depth("keep_random")
    local keep_all = load_setting_depth("keep_all")
    if depth >= 0 then
      local random = current_settings["random"]
      local all = current_settings["all"]
      if keep_random < 0 or random == "" then
        random = nil
        keep_random = nil
      end
      if keep_all < 0 or all == 0 then
        all = nil
        keep_all = nil
      end
      local settings = {
        depth=depth,
        all=all,
        keep_all=keep_all,
        random=random,
        keep_random=keep_random,
        url=url
      }
      url = "custom:"
      for _, k in pairs(
        {'all', 'depth', 'keep_all', 'keep_random', 'random', 'url'}
      ) do
        local v = settings[k]
        if v ~= nil then
          url = url .. k .. "=" .. urlparse.escape(tostring(v)) .. "&"
        end
      end
      url = string.sub(url, 1, -2)
    end
  end
  if not duplicate_urls[url] and not queued_urls[url] then
    if find_path_loop(url, 2) then
      return false
    end
--print('queuing',original, url)
    queued_urls[url] = true
  end
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
  local newurl = string.gsub(url, "([%?&;])[aA][mM][pP];", "%1")
  if url == current_url then
    if newurl ~= url then
      queue_url(newurl)
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
    queue_url(newurl)
  end
  newurl = string.match(newurl, "^([^%?&]+)")
  if newurl ~= url then
    queue_url(newurl)
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

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local parenturl = parent["url"]
  local extract_page_requisites = false

  local current_settings_all = current_settings and current_settings["all"]

  if redirect_urls[parenturl] then
    return true
  end

  if find_path_loop(url, max_repetitions) then
    return false
  end

  local _, count = string.gsub(url, "[/%?]", "")
  if count >= 16 then
    return false
  end

  if string.match(url, "%.pdf") and not string.match(parenturl, "%.pdf") then
    queue_url(url)
    return false
  end

  local domain_match = checked_domains[item_first_url]
  if not domain_match then
    domain_match = check_domain_outlinks(item_first_url)
    if not domain_match then
      domain_match = "none"
    end
    checked_domains[item_first_url] = domain_match
  end
  if domain_match ~= "none" then
    extract_page_requisites = true
    local newurl_domain = string.match(url, "^https?://([^/]+)")
    local to_queue = true
    for domain, _ in pairs(redirect_domains) do
      if check_domain_outlinks(url, domain) then
        to_queue = false
        break
      end
    end
    if to_queue then
      queue_url(url)
      return false
    end
  end

  --[[if not extract_page_requisites then
    return false
  end]]

  if status_code < 200 or status_code >= 300 or not verdict then
    return false
  end

  --[[if string.len(url) == string.len(parenturl) then
    local good_url = false
    local index1, index2
    temp_url = string.match(url, "^https?://(.+)$")
    temp_parenturl = string.match(parenturl, "^https?://(.+)$")
    local start_index = 1
    repeat
      index1 = string.find(temp_url, "/", start_index)
      index2 = string.find(temp_parenturl, "/", start_index)
      if index1 ~= index2 then
        good_url = true
        break
      end
      if index1 then
        start_index = index1 + 1
      end
    until not index1 or not index2
    if not good_url then
      return false
    end
  end]]

  if parenturl_uuid == nil then
    parenturl_uuid = false
    for old_parent_url, _ in pairs(visited_urls) do
      for id_to_ignore, _ in pairs(ids_to_ignore) do
        if string.match(old_parent_url, id_to_ignore) then
          parenturl_uuid = true
          break
        end
      end
      if parenturl_uuid then
        break
      end
    end
  end
  if parenturl_uuid then
    for id_to_ignore, _ in pairs(ids_to_ignore) do
      if string.match(url, id_to_ignore) and not current_settings_all then
        return false
      end
    end
  end

  if urlpos["link_refresh_p"] ~= 0 then
    queue_url(url)
    return false
  end

  if parenturl_requisite == nil then
    parenturl_requisite = false
    for _, pattern in pairs(page_requisite_patterns) do
      for old_parent_url, _ in pairs(visited_urls) do
        if string.match(old_parent_url, pattern) then
          parenturl_requisite = true
          break
        end
      end
      if parenturl_requisite then
        break
      end
    end
  end
  if parenturl_requisite and not current_settings_all then
    return false
  end

  if urlpos["link_inline_p"] ~= 0 then
    queue_url(url)
    return false
  end

  if current_settings_all then
    queue_url(url, true)
    return false
  end

  --[[for old_parent_url, _ in pairs(visited_urls) do
    for _, pattern in pairs(page_requisite_patterns) do
      if string.match(old_parent_url, pattern) then
        return false
      end
    end
  end

  for _, pattern in pairs(page_requisite_patterns) do
    if string.match(url, pattern) then
      queue_url(url)
      return false
    end
  end]]
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local html = nil

  downloaded[url] = true

  local function check(url, headers)
    local url = string.match(url, "^([^#]+)")
    url = string.gsub(url, "&amp;", "&")
    queue_url(url)
  end

  local function checknewurl(newurl, headers)
    if string.match(newurl, "^#") then
      return nil
    end
    if string.match(newurl, "\\[uU]002[fF]") then
      return checknewurl(string.gsub(newurl, "\\[uU]002[fF]", "/"), headers)
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"), headers)
    elseif string.match(newurl, "^https?://") then
      check(newurl, headers)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""), headers)
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""), headers)
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl), headers)
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl), headers)
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl), headers)
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"), headers)
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl), headers)
    end
  end

  local function checknewshorturl(newurl, headers)
    if string.match(newurl, "^#") then
      return nil
    end
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl), headers)
    elseif not (string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^%${")) then
      check(urlparse.absolute(url, newurl), headers)
    else
      checknewurl(newurl, headers)
    end
  end

  if status_code == 200 and current_settings and current_settings["deep_extract"] then
    print('deep extract')
    html = read_file(file)
    for newurl in string.gmatch(html, "[^%-][hH][rR][eE][fF]='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-][hH][rR][eE][fF]="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&[qQ][uU][oO][tT];", '"'), '"(https?://[^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "'(https?://[^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
    --[[for newurl in string.gmatch(html, "%(([^%)]+)%)") do
      checknewurl(newurl)
    end]]
  end
end

wget.callbacks.write_to_warc = function(url, http_stat)
  if bad_code(http_stat["statcode"]) then
    return false
  elseif http_stat["statcode"] >= 300 and http_stat["statcode"] <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if string.match(newloc, "^https?://[^/]*google%.com/sorry")
      or string.match(newloc, "^https?://[^/]*google%.com/[sS]ervice[lL]ogin")
      or string.match(newloc, "^https?://consent%.youtube%.com/")
      or string.match(newloc, "^https?://consent%.google%.com/")
      or string.match(newloc, "^https?://misuse%.ncbi%.nlm%.nih%.gov/")
      or string.match(newloc, "^https?://myprivacy%.dpgmedia%.nl/") then
      report_bad_url(url["url"])
      exit_url = true
      return false
    end
    return true
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

  parenturl_uuid = nil
  parenturl_requisite = nil

  local url_lower = string.lower(url["url"])
  if urls[url_lower] then
    current_url = url_lower
    current_settings = urls_settings[url_lower]
  end

  if status_code >= 200 then
    queue_url(string.match(url["url"], "^(https?://[^/]+)") .. "/robots.txt")
  end

  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. "  \n")
  io.stdout:flush()

  if redirect_domains["done"] then
    redirect_domains = {}
    redirect_urls = {}
    visited_urls = {}
    item_first_url = nil
  end
  redirect_domains[string.match(url["url"], "^https?://([^/]+)")] = true
  if not item_first_url then
    item_first_url = url["url"]
  end

  visited_urls[url["url"]] = true

  if exit_url then
    exit_url = false
    return wget.actions.EXIT
  end

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    redirect_urls[url["url"]] = true
    --[[if strip_url(url["url"]) == strip_url(newloc) then
      queued_urls[newloc] = true
      return wget.actions.EXIT
    end]]
    if downloaded[newloc] then
      return wget.actions.EXIT
    end
  else
    redirect_domains["done"] = true
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
  local dup_urls = io.open(item_dir .. '/' .. warc_file_base .. '_duplicate-urls.txt', 'w')
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
      dup_urls:write(url .. "\n")
      if newurls == nil then
        newurls = url
      else
        newurls = newurls .. "\0" .. url
      end
    end
  end
  dup_urls:close()

  if newurls ~= nil then
    local tries = 0
    while tries < 10 do
      local body, code, headers, status = http.request(
        "http://blackbird-amqp.meo.ws:23038/urls-m92hrwe0faimbhi/",
        newurls
      )
      if code == 200 or code == 409 then
        io.stdout:write("Submitted discovered URLs.\n")
        io.stdout:flush()
        break
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    if tries == 12 then
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

