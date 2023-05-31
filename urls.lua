local urlparse = require("socket.url")
local http = require("socket.http")
local idn2 = require("idn2")
local html_entities = require("htmlEntities")
JSON = (loadfile "JSON.lua")()

local item_dir = os.getenv("item_dir")
local item_name = os.getenv("item_name")
local custom_items = os.getenv("custom_items")
local warc_file_base = os.getenv("warc_file_base")

local url_count = 0
local downloaded = {}
local abortgrab = false
local killgrab = false
local exit_url = false
local min_dedup_mb = 5

local timestamp = nil

local current_file = nil
local current_file_html = nil
local current_file_url = nil

if urlparse == nil or http == nil or html_entities == nil then
  io.stdout:write("Dependencies not corrently installed.\n")
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
for _, lengths in pairs({{8, 4, 4, 4, 12}, {8, 4, 4, 12}}) do
  local uuid = ""
  for _, i in pairs(lengths) do
    for j=1,i do
      uuid = uuid .. "[0-9a-fA-F]"
    end
    if i ~= 12 then
      uuid = uuid .. "%-"
    end
  end
  ids_to_ignore[uuid] = true
end
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
local skip_parent_urls_check = {}
local skip_parent_urls = {}
local remove_params = {}
local filter_discovered = {}
local exit_url_patterns = {}
local page_requisite_patterns = {}
local duplicate_urls = {}
local extract_outlinks_domains = {}
local paths = {}
local extract_from_domain = {}
local item_first_url = nil
local redirect_domains = {}
local checked_domains = {}
local tlds = {}

local year_month = os.date("%Y", timestamp) .. tostring(math.floor(os.date("*t").yday))
local periodic_shard = "periodic" .. year_month

local filter_pattern_sets = {
  ["^https?://[a-z0-9]+%.[^%.]+%.[a-z]+/"]={
    ["pics"]="^https?://[a-z0-9]+%.[^%.]+%.[a-z]+/pics/[a-zA-Z0-9%-_]+%.[a-z]+$",
    --["vicom"]="/[vV][iI]com[0-9]+/",
    ["k8"]="^https?://[kK]8"
  },
  ["^https?://[a-z0-9]+%.[^%.]+%.de/pages/"]={
    ["pages"]="^https?://[a-z0-9]+%.[^%.]+%.de/pages/.+%.html",
    --["css"]="%s.*%.css$"
  },
  ["^https?://[a-z0-9]+%.[^%.]+%.de/news/"]={
    ["news"]="^https?://[a-z0-9]+%.[^%.]+%.de/news/[^&%?/]+$",
    --["css"]="%.css"
  },
  ["^https?://[a-z0-9]+%.[^%.]+%.[a-z]+/.*%%20"]={
    ["space"]={
      "^https?://[a-z0-9]+%.[^%.]+%.[a-z]+/.*%s",
      "^https?://[a-z0-9]+%.[^%.]+%.[a-z]+/.*%%20"
    }
  },
  ["^https?://[^/]+.*/X[a-z0-9]+"]={
    ["x"]="^https?://[^/]+.*/X[a-z0-9]+.*/X[a-z0-9]",
    ["tk88"]="[tT][kK]88"
  },
  ["^http://"]={
    ["index-robots"]={
      "^https?://[^/]+//index%.php%?robots%.txt$",
      "^https?://[^/]+/robots%.txt$"
    },
    ["index-sitemap"]={
      "^https?://[^/]+//index%.php%?sitemap%.xml$",
      "^https?://[^/]+/sitemap%.xml$"
    },
    ["index-html"]={
      "^https?://[^/]+//index%.php%?[a-z]+/[0-9]+%.html$",
      "^https?://[^/]+/[0-9]+%.html$"
    },
    ["images"]="^https?://[^/]+/uploads/images/[0-9]+%.jpg$",
    ["html"]="^https?://[^/]+/[a-z]+/[0-9]+%.html$",
    ["page"]="^https?://[^/]+/[a-z]+/[0-9]+/$"
  },
  ["^http://[a-z0-9]+%.[^%.]+%.[a-z]+/$"]={
    ["sinaimgcn"]="^http://n%.sinaimg%.cn/.+%.jpg$",
    ["sitemap"]="^http://[a-z0-9]+%.[^%.]+%.[a-z]+/sitemap%.xml",
    ["template"]="/template/default/04190%-44/",
    ["domain"]="^http://[a-z0-9]+%.[^%.]+%.[a-z]+/$"
  },
  ["^https?://[a-z0-9]+%.[^%.]+%.[a-z]+/.?"]={
    ["url"]={
      "^https?://[a-z0-9]+%.[^%.]+%.[a-z]+/_static_index/",
      "^https?://[a-z0-9]+%.[^%.]+%.[a-z]+.*/spring_php/",
      "^https?://[a-z0-9]+%.[^%.]+%.[a-z]+.*/count_php/",
      "^https?://[a-z0-9]+%.[^%.]+%.[a-z]+.*/plus/.*[_%.]php"
    },
    ["base"]="^https?://[a-z0-9]+%.[^%.]+%.[a-z]+/$",
    ["image"]="^https?://[a-z0-9]+%.[^%.]+%.[a-z]+/fx%-[a-zA-Z0-9=]+/uploads/[0-9]+/[0-9]+/[0-9]+/[0-9]+%.jpg$",
    --["html"]="^https?://[a-z0-9]+%.[^%.]+%.[a-z]+/[a-z0-9]+%.html$",
    --["abouthtml"]="^https?://[a-z0-9]+%.[^%.]+%.[a-z]+/about%.html$",
    --["sitemaphtml"]="^https?://[a-z0-9]+%.[^%.]+%.[a-z]+/sitemap%.html$",
    --["announcehtml"]="^https?://[a-z0-9]+%.[^%.]+%.[a-z]+/announce%.html$",
    ["number"]="^https?://[a-z0-9]+%./"
  }
}

local zippyshare_urls_items = {
  [""]={}
}
local pastebin_items = {
  [""]={}
}
local mediafire_items = {
  [""]={}
}
local telegram_posts = {
  [""]={},
  [periodic_shard]={}
}
local telegram_channels = {
  [""]={},
  [periodic_shard]={}
}
local ftp_urls = {[""]={}}
local urls_all = {}

local month_timestamp = os.date("%Y%m", timestamp)

local parenturl_uuid = nil
local parenturl_requisite = nil

local dupes_file = io.open("duplicate-urls.txt", "r")
for url in dupes_file:lines() do
  duplicate_urls[url] = true
end
dupes_file:close()

local tlds_file = io.open("static-tlds.txt", "r")
for tld in tlds_file:lines() do
  tlds[tld] = true
end
tlds_file:close()

local remove_params_file = io.open("static-remove-params.txt", "r")
for param in remove_params_file:lines() do
  local param = string.gsub(
    param, "([a-zA-Z])",
    function(c)
      return "[" .. string.lower(c) .. string.upper(c) .. "]"
    end
  )
  table.insert(remove_params, param)
end
remove_params_file:close()

local filter_discovered_file = io.open("static-filter-discovered.txt", "r")
for pattern in filter_discovered_file:lines() do
  table.insert(filter_discovered, pattern)
end
filter_discovered_file:close()

local exit_url_patterns_file = io.open("static-exit-url-patterns.txt", "r")
for pattern in exit_url_patterns_file:lines() do
  table.insert(exit_url_patterns, pattern)
end
exit_url_patterns_file:close()

local page_requisite_patterns_file = io.open("static-page-requisite-patterns.txt", "r")
for pattern in page_requisite_patterns_file:lines() do
  table.insert(page_requisite_patterns, pattern)
end
page_requisite_patterns_file:close()

--[[local extract_outlinks_domains_file = io.open("static-extract-outlinks-domains.txt", "r")
for pattern in extract_outlinks_domains_file:lines() do
  extract_outlinks_domains[pattern] = true
end
extract_outlinks_domains_file:close()]]

local paths_file = io.open("static-paths.txt", "r")
for line in paths_file:lines() do
  paths[line] = true
end
paths_file:close()

local extract_from_domain_file = io.open("static-extract-from-domain.txt", "r")
for pattern in extract_from_domain_file:lines() do
  if not string.match(pattern, "^#") then
    extract_from_domain[pattern] = true
  end
end
extract_from_domain_file:close()

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  killgrab = true
end

read_file = function(file, bytes)
  if not bytes then
    bytes = "*all"
  end
  if file then
    local f = assert(io.open(file))
    local data = f:read(bytes)
    f:close()
    if not data then
      data = ""
    end
    return data
  else
    return ""
  end
end

table_length = function(t)
  local count = 0
  if not t then
    return count
  end
  for _ in pairs(t) do
    count = count + 1
  end
  return count
end

check_domain_outlinks = function(url, target)
  local parent = string.match(url, "^https?://([^/]+)")
  while parent do
    if (not target and extract_outlinks_domains[parent])
      or (target and parent == target) then
      return parent
    end
    parent = string.match(parent, "^[^%.]+%.(.+)$")
  end
  return false
end

bad_code = function(status_code)
  return status_code ~= 200
    and status_code ~= 301
    and status_code ~= 302
    and status_code ~= 303
    and status_code ~= 307
    and status_code ~= 308
    and status_code ~= 404
    and status_code ~= 410
end

find_path_loop = function(url, max_repetitions)
  local tested = {}
  local tempurl = urlparse.unescape(url)
  tempurl = string.match(tempurl, "^https?://[^/]+(.*)$")
  if not tempurl then
    return false
  end
  for s in string.gmatch(tempurl, "([^/%?&]+)") do
    s = string.lower(s)
    if not tested[s] then
      if s == "" then
        tested[s] = -2
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

percent_encode_url = function(url)
  temp = ""
  for c in string.gmatch(url, "(.)") do
    local b = string.byte(c)
    if b < 32 or b > 126 then
      c = string.format("%%%02X", b)
    end
    temp = temp .. c
  end
  return temp
end

queue_url = function(url, withcustom)
  if not url then
    return nil
  end
  --[[if string.match(url, "^http://")
    and string.match(current_url, "^http://")
    and string.match(url, "^http://([^/]+)") ~= string.match(current_url, "^http://([^/]+)") then
    return nil
  end]]
  queue_new_urls(url)
  if not string.match(url, "^https?://[^/]+%.") then
    return nil
  end
--local original = url
  load_setting_depth = function(s)
    n = tonumber(current_settings[s])
    if n == nil then
      n = 0
    end
    return n - 1
  end
  url = string.gsub(url, "'%s*%+%s*'", "")
  url = percent_encode_url(url)
  url = string.match(url, "^([^#{<\\]+)")
  if current_settings and current_settings["all"] and withcustom then
    local depth = load_setting_depth("depth")
    local keep_random = load_setting_depth("keep_random")
    local keep_all = load_setting_depth("keep_all")
    local any_domain = load_setting_depth("any_domain")
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
      if any_domain <= 0 then
        any_domain = nil
      end
      local settings = {
        depth=depth,
        all=all,
        keep_all=keep_all,
        random=random,
        keep_random=keep_random,
        url=url,
        any_domain=any_domain
      }
      url = "custom:"
      for _, k in pairs(
        {"all", "any_domain", "depth", "keep_all", "keep_random", "random", "url"}
      ) do
        local v = settings[k]
        if v ~= nil then
          url = url .. k .. "=" .. urlparse.escape(tostring(v)) .. "&"
        end
      end
      url = string.sub(url, 1, -2)
    end
  end
  for _, pattern in pairs(filter_discovered) do
    if string.match(url, pattern) then
      return false
    end
  end
  local shard = ""
  if string.match(url, "&random=") then
    shard = "periodic"
  end
  if not queued_urls[shard] then
    queued_urls[shard] = {}
  end
  if not duplicate_urls[url] and not queued_urls[shard][url] then
    if find_path_loop(url, 2) then
      return false
    end
--print("queuing",original, url)
    queued_urls[shard][url] = current_url
  end
end

queue_monthly_item = function(item, t)
  if not t[month_timestamp] then
    t[month_timestamp] = {}
  end
--print("monthly", url)
  t[month_timestamp][item] = current_url
end

queue_monthly_url = function(url)
  if find_path_loop(url, 3) then
    return nil
  end
  url = percent_encode_url(url)
  url = string.match(url, "^([^#]+)")
  queue_monthly_item("custom:random=" .. month_timestamp .. "&url=" .. urlparse.escape(tostring(url)), queued_urls)
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
  if not url then
    return nil
  end
  local newurl = string.gsub(url, "([%?&;])[aA][mM][pP];", "%1")
  if url == current_url then
    if newurl ~= url then
      queue_url(newurl)
    end
  end
  for _, param_pattern in pairs(remove_params) do
    newurl = remove_param(newurl, param_pattern)
  end
  if newurl ~= url then
    queue_url(newurl)
  end
  newurl = string.match(newurl, "^([^%?&]+)")
  if newurl ~= url then
    queue_url(newurl)
  end
  url = string.gsub(url, "&quot;", '"')
  url = string.gsub(url, "&amp;", "&")
  for newurl in string.gmatch(url, '([^"\\]+)') do
    if newurl ~= url then
      queue_url(newurl)
    end
  end
  --[[for newurl in string.gmatch(url, "(https?%%3[aA]%%2[fF][^%?&;]+)") do
    newurl = urlparse.unescape(newurl)
    queue_url(newurl)
  end
  for _, pattern in pairs({
    ".(https?:/[^&%?;]+)",
    ".(https?:/[^&%?]+)",
    ".(https?:/[^&]+)",
    ".(https?:/.+)"
  }) do
    for newurl in string.gmatch(url, pattern) do
      queue_url(newurl)
    end
  end
  if string.match(url, "^https?:/[^/]") then
    queue_url(string.gsub(url, "^(https?:/)(.+)", "%1/%2"))
  end]]
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

queue_telegram = function(rest)
  local rest = string.match(rest, "^([^%?&]+)")
  if string.len(rest) == 0 then
    return nil
  end
  local _, temp = string.match(rest, "^/s(/.+)$")
  if temp then
    rest = temp
  end
  local user = string.match(rest, "^/([^/]+)")
  if user then
    telegram_posts[periodic_shard]["channel:" .. user] = true
    telegram_channels[""]["channel:" .. user] = true
  else
    return nil
  end
  local post = string.match(rest, "^/[^/]+/([0-9]+)$")
  if post then
    telegram_posts[periodic_shard]["post:" .. user .. ":" .. post] = true
  end
end

queue_pastebin = function(rest)
  for s in string.gmatch(rest, "([a-zA-Z0-9]+)") do
    if string.len(s) == 8 then
      pastebin_items[""][s] = true
    end
  end
end

queue_mediafire = function(rest)
  for s in string.gmatch(rest, "([a-zA-Z0-9]+)") do
    s = string.lower(s)
    if string.len(s) >= 10 then
      mediafire_items[""]["id:" .. s] = true
      s = string.match(s, "^(.-).g$")
      if s then
        mediafire_items[""]["id:" .. s] = true
      end
    end
  end
end

queue_services = function(url)
  if not url then
    return nil
  end
  local domain, rest = string.match(url, "^https?://[^/]-([^/%.]+%.[^/%.]+)(/.*)$")
  if domain == "t.me" or domain == "telegram.me" then
    queue_telegram(rest)
  elseif domain == "pastebin.com" then
    queue_pastebin(rest)
  elseif domain == "mediafire.com" or domain == "mfi.re" then
    queue_mediafire(rest)
  elseif domain == "zippyshare.com" then
    zippyshare_urls_items[""][url] = true
  end
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local parenturl = parent["url"]
  local extract_page_requisites = false

  local current_settings_all = current_settings and current_settings["all"]
  local current_settings_any_domain = current_settings and current_settings["any_domain"]

  queue_services(url)

  if string.match(url, "^ftp://") then
    ftp_urls[""][url] = true
    return false
  end

  if skip_parent_urls[parenturl] then
    return false
  end

  if url ~= parenturl then
    for parenturl_pattern, pattern_table in pairs(filter_pattern_sets) do
      if string.match(parenturl, parenturl_pattern) then
        local found_any = false
        local check_string = parenturl_pattern .. parenturl
        if not skip_parent_urls_check[check_string] then
          skip_parent_urls_check[check_string] = {}
          for k, _ in pairs(pattern_table) do
            skip_parent_urls_check[check_string][k] = false
          end
        end
        for pattern_name, patterns in pairs(pattern_table) do
          if not skip_parent_urls_check[check_string][pattern_name] then
            if type(patterns) == "string" then
              patterns = {patterns}
            end
            for _, pattern in pairs(patterns) do
              if string.match(url, pattern) then
                found_any = true
                skip_parent_urls_check[check_string][pattern_name] = true
                break
              end
            end
          end
        end
        if found_any then
          local all_true = true
          for _, v in pairs(skip_parent_urls_check[check_string]) do
            if not v then
              all_true = false
              break
            end
          end
          if all_true then
            io.stdout:write("Skipping all URLs discovered for URL " .. parenturl .. ".\n")
            io.stdout:flush()
            skip_parent_urls[parenturl] = true
          end
        end
      end
    end
  end

  --queue_monthly_item(url, urls_all)
  --queue_monthly_url(string.match(url, "^(https?://[^/]+)") .. "/")

  if redirect_urls[parenturl] and not (
    status_code == 300 and string.match(parenturl, "^https?://[^/]*feb%-web%.ru/")
  ) then
    return true
  end

  if (
      string.match(parenturl, "^https://[^/%.]+%.[^/%.]+%.de/en/")
      or string.match(parenturl, "^https://[^/%.]+%.[^/%.]+%.de/mobile/")
      or string.match(parenturl, "^https://[^/%.]+%.[^/%.]+%.de/page/")
      or (
        string.match(parenturl, "^https://[^/%.]+%.[^/%.]+%.de/")
        and (
          string.match(parenturl, "%%20")
          or string.match(parenturl, "_desktop_img/")
        )
      )
    )
    and (
      string.match(url, "^https://[^/%.]+%.[^/%.]+%.de/en/")
      or string.match(url, "^https://[^/%.]+%.[^/%.]+%.de/mobile/")
      or string.match(url, "^https://[^/%.]+%.[^/%.]+%.de/page/")
      or (
        string.match(url, "^https://[^/%.]+%.[^/%.]+%.de/")
        and (
          string.match(url, " ")
          or string.match(url, "%%20")
          or string.match(url, "_desktop_img/")
        )
      )
    )
    and (
      string.match(parenturl, "[/%.][a-z0-9]+$")
      or string.match(parenturl, "[/%.][a-z0-9]+%?")
    )
    and (
      string.match(url, "[/%.][a-z0-9]+$")
      or string.match(url, "[/%.][a-z0-9]+%?")
    ) then
    return false
  end

  -- prevent loop on some bad srcset URLs
  if parenturl == current_file_url
    and (
      string.match(url, "https?://[a-z]+%.[^%.]+%.[a-z]+/[a-z]+$")
      or string.match(url, "https?://[a-z]+%.[^%.]+%.[a-z]+/en/[a-z]+$")
    ) then
    if current_file_html == nil then
      current_file_html = read_file(current_file)
    end
    for srcset in string.gmatch(current_file_html, 'srcset="([^"]+)"') do
      for srcset_url in string.gmatch(srcset, "([^,]+)") do
        if string.match(url, "([a-z]+)$") == string.match(srcset_url, "^%s*([a-z]+)%s[^%s].-%s[0-9]+w$") then
          return false
        end
      end
    end
  end

  if string.match(parenturl, "^https?://[^/]+/$") then
    for s in string.gmatch(string.match(url, "^https?://[^/]+(.*)$"), "([a-zA-Z0-9]+)") do
      if extract_from_domain[s] then
        queue_monthly_url(url)
      end
    end
  end

  if find_path_loop(url, 2) then
    return false
  end

  local _, count = string.gsub(url, "[/%?]", "")
  if count >= 16 then
    return false
  end

  if string.match(parenturl, "%?content=[a-zA-Z0-9%%]+$")
    and string.match(url, "%?content=[a-zA-Z0-9%%]+$") then
    return false
  end

  for _, extension in pairs({
    "pdf",
    "doc[mx]?",
    "xls[mx]?",
    "ppt[mx]?",
    "dot[mx]?",
    "pot[mx]?",
    "pps[mx]?",
    "xlt[mx]?",
    "txt",
    "rtf",
    "jar",
    "swf",
    "csv",
    --"zip",
    "odt",
    "odm",
    "ods",
    "odp",
    "xml",
    "json",
    "torrent"
  }) do
    local prefix = "[%.%?/&]"
    if string.match(parenturl, prefix .. extension .. "$")
      or string.match(parenturl, prefix .. extension .. "[^a-z0-9A-Z]")
      or string.match(parenturl, prefix .. string.upper(extension) .. "$")
      or string.match(parenturl, prefix .. string.upper(extension) .. "[^a-z0-9A-Z]")
      -- get rid of loop on sites from chinese origin (also various non-.cn domains)
      or string.match(url, "^https?://[^/]+/%?/.+%." .. extension .. "$")
      or string.match(url, "^https?://[^/]+/.+%.[a-z]+%?/.+%." .. extension .. "$") then
      return false
    end
    if string.match(url, prefix .. extension .. "$")
      or string.match(url, prefix .. extension .. "[^a-z0-9A-Z]")
      or string.match(url, prefix .. string.upper(extension) .. "$")
      or string.match(url, prefix .. string.upper(extension) .. "[^a-z0-9A-Z]") then
      queue_url(url)
      return false
    end
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

  if (status_code < 200 or status_code >= 300 or not verdict)
    and not current_settings_all then
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

  if urlpos["link_inline_p"] ~= 0
    and not string.match(url, "%.html$") then
    queue_url(url)
    return false
  end

  local current_host = string.match(urlpos["url"]["host"], "([^%.]+%.[^%.]+)$")
  local first_parent_host = string.match(parent["host"], "([^%.]+%.[^%.]+)$")

  if current_url then
    first_parent_host = string.match(current_url .. "/", "^https?://[^/]-([^/%.]+%.[^/%.]+)/")
  end

  if current_settings_all and (
    current_settings_any_domain
    or first_parent_host == current_host
  ) then
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

  queue_services(url)

  if url then
    downloaded[url] = true
  end

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
    elseif not url then
      return nil
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
    if url and string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl), headers)
    elseif url and not (string.match(newurl, "^https?:\\?/\\?//?/?")
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

  if (status_code == 200 and current_settings and current_settings["deep_extract"])
    or not url then
    html = read_file(file)
    if not url then
      html = string.gsub(html, "&#160;", " ")
      html = string.gsub(html, "&lt;", "<")
      html = string.gsub(html, "&gt;", ">")
      html = string.gsub(html, "&quot;", '"')
      html = string.gsub(html, "&apos;", "'")
      for _, pattern in pairs({
        "https?://www([^\032-\126]+)",
        "https?://[^/%.]+([^\032-\126]+)[^/%.]+/"
      }) do
        for s in string.gmatch(html, pattern) do
          --print("replacing", s)
          html = string.gsub(html, s, "%.")
        end
      end
      html = string.gsub(html, "&#(%d+);",
        function(n)
          return string.char(n)
        end
      )
      html = string.gsub(html, "&#x(%d+);",
        function(n)
          return string.char(tonumber(n, 16))
        end
      )
      local temp = html
      for _, remove in pairs({"", "<br/>", "</?p[^>]*>"}) do
        if remove ~= "" then
          temp = string.gsub(temp, remove, "")
        end
        temp2 = string.gsub(temp, "%s+\n", "\n")
        temp2 = string.gsub(temp2, "([^>\"'\\`}%)%]%.,])\n", "%1")
        for _, newline_white in pairs({" ", ""}) do
          temp3 = string.gsub(temp2, "\n", newline_white)
          local url_patterns = {
            "([hH][tT][tT][pP][sS]?://[^%s<>#\"'\\`{}%)%]]+)",
            '"([hH][tT][tT][pP][sS]?://[^"]+)',
            "'([hH][tT][tT][pP][sS]?://[^']+)",
            ">%s*([hH][tT][tT][pP][sS]?://[^<%s]+)"
          }
          if newline_white == " " then
            table.insert(url_patterns, "([a-zA-Z0-9%-%.]+%.[a-zA-Z0-9%-%.]+)")
            table.insert(url_patterns, "([a-zA-Z0-9%-%.]+%.[a-zA-Z0-9%-%.]+/[^%s<>#\"'\\`{}%)%]]+)")
          end
          for _, pattern in pairs(url_patterns) do
            for newurl in string.gmatch(temp3, pattern) do
              while string.match(newurl, ".[%.&,!;]$") do
                newurl = string.match(newurl, "^(.+).$")
              end
              if string.match(newurl, "^[hH][tT][tT][pP][sS]?://") then
                local a, b = string.match(newurl, "^([hH][tT][tT][pP][sS]?://[^/]+)(.*)")
                newurl = string.lower(a) .. b
                check(newurl)
                check(html_entities.decode(newurl))
              elseif string.match(newurl, "^[a-zA-Z0-9]") then
                if not string.find(newurl, "/") then
                  newurl = newurl .. "/"
                end
                local a, b = string.match(newurl, "^([^/]+)(/.*)$")
                newurl = string.lower(a) .. b
                local tld = string.match(newurl, "^[^/]+%.([a-z]+)/")
                if not tld then
                  tld = string.match(newurl, "^[^/]+%.(xn%-%-[a-z0-9]+)/")
                end
                --print(newurl, tld, tlds[tld])
                if tld and tlds[tld] then
                  check("http://" .. newurl)
                  check("https://" .. newurl)
                end
              end
            end
          end
        end
      end
    end
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
    if url then
      for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
        checknewurl(newurl)
      end
    end
    --[[for newurl in string.gmatch(html, "%(([^%)]+)%)") do
      checknewurl(newurl)
    end]]
  end
  if url then
    if string.match(url, "^https?://[^/]+/.*[^a-z0-9A-Z][pP][dD][fF]$")
      or string.match(url, "^https?://[^/]+/.*[^a-z0-9A-Z][pP][dD][fF][^a-z0-9A-Z]")
      or string.match(read_file(file, 4), "%%[pP][dD][fF]") then
      io.stdout:write("Extracting links from PDF.\n")
      io.stdout:flush()
      local temp_file = file .. "-html.html"
      local check_file = io.open(temp_file)
      if check_file then
        check_file:close()
        os.remove(temp_file)
      end
      os.execute("pdftohtml -nodrm -hidden -i -s -q " .. file)
      check_file = io.open(temp_file)
      if check_file then
        check_file:close()
        local temp_length = table_length(queued_urls[""])
        wget.callbacks.get_urls(temp_file, nil, nil, nil)
        io.stdout:write("Found " .. tostring(table_length(queued_urls[""])-temp_length) .. " URLs.\n")
        io.stdout:flush()
        os.remove(temp_file)
      else
        io.stdout:write("Not a PDF.\n")
        io.stdout:flush()
      end
    end
    if status_code == 200 then
      if string.match(url, "^https?://[^/]+/robots%.txt$")
        or string.match(url, "^https?://[^/]+/security%.txt$")
        or string.match(url, "^https?://[^/]+/%.well%-known/security%.txt$") then
        html = read_file(file) .. "\n"
        if not string.match(html, "<[^>]+/>")
          and not string.match(html, "</") then
          for line in string.gmatch(html, "(.-)\n") do
            local name, path = string.match(line, "([^:]+):%s*(.-)%s*$")
            if name and path then
              -- the path should normally be absolute already
              local newurl = urlparse.absolute(url, path)
              if string.lower(name) == "sitemap" then
                queue_monthly_url(newurl)
              elseif string.lower(name) ~= "user-agent"
                and not string.match(path, "%*")
                and not string.match(path, "%$") then
                queue_url(newurl)
              end
            end
          end
        end
      elseif string.match(url, "^https?://[^/]+/ads%.txt$")
        or string.match(url, "^https?://[^/]+/app%-ads%.txt$") then
        html = read_file(file) .. "\n"
        for line in string.gmatch(html, "(.-)\n") do
          if not string.match(line, "^#") then
            local site = string.match(line, "^([^,%s]+),")
            if site then
              if string.match(site, "^https?://") then
                queue_url(site)
              else
                queue_url("http://" .. site .. "/")
                queue_url("https://" .. site .. "/")
              end
            end
          end
        end
      elseif string.match(url, "^https?://[^/]+/%.well%-known/trust%.txt$") then
        html = read_file(file) .. "\n"
        for line in string.gmatch(html, "(.-)\n") do
          if not string.match(line, "^#") then
            local a, b = string.match(line, "^([^=]+)=%s*(https?://.-)%s*$")
            if b then
              queue_url(b)
            end
          end
        end
      elseif string.match(url, "^https?://[^/]+/%.well%-known/nodeinfo$")
        or string.match(url, "^https?://[^/]+/%.well%-known/openid%-configuration$")
        or string.match(url, "^https?://[^/]+/%.well%-known/ai%-plugin%.json$") then
        html = read_file(file)
        html = string.gsub(html, "\\", "")
        for s in string.gmatch(html, '([^"]+)') do
          if string.match(s, "^https?://") then
            queue_monthly_url(s)
          end
        end
      end
    end
    if string.match(url, "sitemap.*%.gz$")
      or string.match(url, "%.xml%.gz") then
      local temp_file = file .. ".uncompressed"
      io.stdout:write("Attempting to decompress sitemap to " .. temp_file .. ".\n")
      io.stdout:flush()
      os.execute("gzip -kdc " .. file .. " > " .. temp_file)
      local check_file = io.open(temp_file)
      if check_file then
        io.stdout:write("Extracting sitemaps from decompressed sitemap.\n")
        io.stdout:flush()
        check_file:close()
        wget.callbacks.get_urls(temp_file, string.match(url, "^(.-)%.gz$"), nil, nil)
      end
    end
    if string.match(url, "^https?://[^/]+/.*%.[xX][mM][lL]")
      and string.match(string.lower(read_file(file, 200)), "sitemap") then
      html = read_file(file)
      for sitemap in string.gmatch(html, "<sitemap>(.-)</sitemap>") do
        local newurl = string.match(sitemap, "<loc>%s*([^%s<]+)%s*</loc>")
        newurl = html_entities.decode(newurl)
        if newurl then
          -- should already be absolute
          newurl = urlparse.absolute(url, newurl)
          queue_monthly_url(newurl)
        end
      end
    end
  end
end

wget.callbacks.write_to_warc = function(url, http_stat)
  local url_lower = string.lower(url["url"])
  current_file = http_stat["local_file"]
  current_file_url = url["url"]
  current_file_html = nil
  if urls[url_lower] then
    current_url = url_lower
    current_settings = urls_settings[url_lower]
  end
  if current_settings and not current_settings["random"] then
    queue_url(url["url"])
    return false
  end
  if bad_code(http_stat["statcode"]) then
    return false
  elseif http_stat["statcode"] >= 300 and http_stat["statcode"] <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if string.match(newloc, "^https?://[^/]*google%.com/sorry")
      or string.match(newloc, "^https?://[^/]*google%.com/[sS]ervice[lL]ogin")
      or string.match(newloc, "^https?://consent%.youtube%.com/")
      or string.match(newloc, "^https?://consent%.google%.com/")
      or string.match(newloc, "^https?://misuse%.ncbi%.nlm%.nih%.gov/")
      or string.match(newloc, "^https?://myprivacy%.dpgmedia%.nl/")
      or string.match(newloc, "^https?://idp%.springer%.com/authorize%?")
      or string.match(newloc, "^https?://[^/]*instagram%.com/accounts/") then
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
  if http_stat["len"] > min_dedup_mb * 1024 * 1024 then
    io.stdout:write("Data larger than " .. tostring(min_dedup_mb) .. " MB. Checking with Wayback Machine.\n")
    io.stdout:flush()
    while true do
      local body, code, headers, status = http.request(
        "https://web.archive.org/__wb/calendarcaptures/2"
          .. "?url=" .. urlparse.escape(url["url"])
          .. "&date=202"
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

  if not timestamp then
    local body, code, headers, status = http.request("https://legacy-api.arpa.li/now")
    assert(code == 200)
    timestamp = tonumber(string.match(body, "^([0-9]+)"))
  end


  if status_code ~= 0 then
    local base_url = string.match(url["url"], "^(https://[^/]+)")
    if base_url then
      for path, _ in pairs(paths) do
        queue_monthly_url(base_url .. path)
      end
    end
  end

  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. "  \n")
  io.stdout:flush()

  if killgrab then
    return wget.actions.ABORT
  end

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
      queued_urls[""][newloc] = true
      return wget.actions.EXIT
    end]]
    if downloaded[newloc]
      or string.match(newloc, "^magnet:") then
      return wget.actions.EXIT
    elseif string.match(url["url"], "^https?://[^/]*telegram%.org/dl%?tme=")
      or (
        string.match(newloc, "^https?://www%.(.+)")
        or string.match(newloc, "^https?://(.+)")
      ) == (
        string.match(url["url"], "^https?://www%.(.+)")
        or string.match(url["url"], "^https?://(.+)")
      )
      or status_code == 301
      or status_code == 303
      or status_code == 308 then
      queue_url(newloc)
      return wget.actions.EXIT
    end
  else
    redirect_domains["done"] = true
  end

  if downloaded[url["url"]] then
    report_bad_url(url["url"])
    return wget.actions.EXIT
  end

  for _, pattern in pairs(exit_url_patterns) do
    if string.match(url["url"], pattern) then
      return wget.actions.EXIT
    end
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
  local function submit_backfeed(newurls, key, shard)
    local tries = 0
    local maxtries = 10
    local parameters = ""
    if shard ~= "" then
      parameters = "?shard=" .. shard
    end
    while tries < maxtries do
      if killgrab then
        return false
      end
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key .. parameters,
        newurls .. "\0"
      )
      print(body)
      if code == 200 then
        io.stdout:write("Submitted discovered URLs.\n")
        io.stdout:flush()
        break
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    if tries == maxtries then
      kill_grab()
    end
  end

  local dup_urls = io.open(item_dir .. "/" .. warc_file_base .. "_duplicate-urls.txt", "w")
  for key, items_data in pairs({
    ["telegram-wdvrpbeov02cm53"] = telegram_posts,
    ["telegram-channels-c8cixci89uv1exw"] = telegram_channels,
    ["pastebin-xa5xj5bx2no3qc1"] = pastebin_items,
    ["mediafire-9cmzz6b3jawqbih"] = mediafire_items,
    ["urls-glx7ansh4e17aii"] = queued_urls,
    ["ftp-urls-en2fk0pjyxljsf9"] = ftp_urls,
    ["urls-all-tx2vacclx396i0h"] = urls_all,
    ["zippyshare-urls-jtelkase24jmz0z"] = zippyshare_urls_items
  }) do
    local project_name = string.match(key, "^(.+)%-")
    for shard, url_data in pairs(items_data) do
      local count = 0
      local newurls = nil
      local is_bad = false
      print("Queuing to project " .. project_name .. " on shard " .. shard)
      for url, parent_url in pairs(url_data) do
        if not is_bad and not skip_parent_urls[parent_url] then
          io.stdout:write("Queuing URL " .. url .. " from " .. parent_url .. ".\n")
          io.stdout:flush()
          if shard == "" and project_name == "urls" then
            dup_urls:write(url .. "\n")
          end
          if newurls == nil then
            newurls = url
          else
            newurls = newurls .. "\0" .. url
          end
          count = count + 1
          if count == 100 then
            submit_backfeed(newurls, key, shard)
            newurls = nil
            count = 0
          end
        end
      end
      if newurls ~= nil then
        submit_backfeed(newurls, key, shard)
      end
    end
  end
  dup_urls:close()

  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-urls.txt", "w")
  for url, _ in pairs(bad_urls) do
    file:write(url .. "\n")
  end
  file:close()
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    return wget.exits.IO_FAIL
  end
  return exit_status
end

