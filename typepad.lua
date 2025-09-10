local urlparse = require("socket.url")
local http = require("socket.http")
local https = require("ssl.https")
local cjson = require("cjson")
local utf8 = require("utf8")
local html_entities = require("htmlEntities")

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local concurrency = tonumber(os.getenv("concurrency"))
local item_type = nil
local item_name = nil
local item_value = nil
local item_user = nil

local url_count = 0
local tries = 0
local downloaded = {}
local seen_200 = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false
local logged_response = false

local discovered_outlinks = {}
local discovered_items = {}
local discovered_extra_items = {}
local bad_items = {}
local ids = {}
local context = {}

local retry_url = false
local is_initial_url = true

local sites = {}
for _, site in pairs(cjson.decode(os.getenv("sites"))) do
  sites[site] = true
end
local assets = cjson.decode(os.getenv("assets"))

local sizes = {
  ["pi"] = {50, 75, 115, 120, 200, 320, 350, 500, 640, 800, 1024, 1200, 2000},
  ["wi"] = {50, 75, 100, 115, 120, 150, 200, 250, 300, 320, 350, 400, 450, 500, 550, 580, 600, 640, 650, 700, 750, 800, 850, 900, 950, 1024},
  ["hi"] = {75, 250},
  ["si"] = {16, 20, 50, 75, 115, 120, 150, 220, 250},
}

site_included = function(domain)
  local temp = string.match(domain, "^www%.(.+)$")
  if temp then
    domain = temp
  end
  return sites[domain]
end

get_domain_item = function(domain)
  local temp = string.match(domain, "^https?://(.+)$")
  if temp then
    domain = temp
  end
  domain = string.match(domain, "^([^/]+)")
  local temp = string.match(domain, "^([^%.]+)%.typepad%.com$")
  if temp then
    return temp
  end
  if not site_included(domain) then
    return nil
  end
  return domain
end

abort_item = function(item)
  abortgrab = true
  --killgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  killgrab = true
end

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

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

discover_item = function(target, item)
  local temp = string.match(item, "^(asset2:.+)%-[0-9a-z]+$")
  if temp then
    discover_item(discovered_extra_items, temp)
  end
  if not target[item] then
--print("discovered", item)
    target[item] = true
    return true
  end
  return false
end

item_patterns = {
  ["^https?://profile%.typepad%.com/([0-9a-zA-Z]+)$"]={
    ["type"]="profile",
    ["additional"]=function(s)
      if s == "services" then
        return nil
      end
      return {["value"]=s}
    end
  },
  ["^https?://([^/]+)/$"]={
    ["type"]="blog",
    ["additional"]=function(s)
      if s == "www.typepad.com"
        or s == "profile.typepad.com" then
        return nil
      end
      local domain_item = get_domain_item(s)
      if domain_item then
        return {["value"]=domain_item}
      end
    end
  },
  ["^https?://([^/]+/%.a/.+)$"]={
    ["type"]="asset2",
    ["additional"]=function(s)
      local temp, ending = string.match(s, "^(.+/[0-9a-f]+)%-([0-9a-zA-Z]+)$")
      if ending
        and ending == "pi" then
        s = temp
      end
      local image_id = string.match(s, "/([0-9a-f]+)%-?[0-9a-zA-Z]*$")
      if not image_id then
        error("Image ID could not be extracted from " .. s .. ".")
      end
      if context["image_id"] == image_id then
        return nil
      end
      server_name, rest = string.match(s, "^([a-z]+)[0-9]*(%.typepad%.com/.+)$")
      if server_name == "a" or server_name == "up" then
        s = server_name .. "1" .. rest
      end
      if get_domain_item(s) then
        return {
          ["value"]=s,
          ["image_id"]=image_id,
          ["extra_ids"]={image_id}
        }
      end
    end
  },
  ["^https?://(up[0-9]?%.typepad%.com/.+)$"]={
    ["type"]="asset2",
    ["additional"]="^https?://([^/]+/%.a/.+)$"
  },
  ["^https?://(a[0-9]?%.typepad%.com/.+)$"]={
    ["type"]="asset2",
    ["additional"]="^https?://([^/]+/%.a/.+)$"
  },
  ["^https?://(.+/photos/.-%.[^/%.%?&]+)$"]={
    ["type"]="asset",
    ["additional"]=function(s)
      if string.match(s, "%.html?%?") then
        return nil
      end
      local extension = string.match(s, "%.([^/%.%?&]+)$")
      if extension ~= "html"
        and extension ~= "htm"
        and get_domain_item(s) then
        return {["value"]=s}
      end
    end
  },
  ["^https?://(.+/images/.-%.[^/%.%?&]+)$"]={
    ["type"]="asset",
    ["additional"]="^https?://(.+/photos/.-%.[^/%.%?&]+)$"
  },
  ["^https?://(.+/files/.-%.[^/%.%?&]+)$"]={
    ["type"]="asset",
    ["additional"]="^https?://(.+/photos/.-%.[^/%.%?&]+)$"
  },
  ["^https?://(.+/%.a/.-%.[^/%.%?&]+)$"]={
    ["type"]="asset",
    ["additional"]="^https?://(.+/photos/.-%.[^/%.%?&]+)$"
  },
  ["^https?://(.+/%.shared/.-%.[^/%.%?&]+)$"]={
    ["type"]="asset",
    ["additional"]="^https?://(.+/photos/.-%.[^/%.%?&]+)$"
  },
  ["^https?://([^/]+/.+%.[0-9a-zA-Z]+)$"]={
    ["type"]="asset",
    ["additional"]="^https?://(.+/photos/.-%.[^/%.%?&]+)$"
  },
  ["^https?://([^/]+/%.shared/.+)$"]={
    ["type"]="asset",
    ["additional"]=function(s)
      if string.match(s, "^[^/]+/%.shared/image%.html") then
        return nil
      end
      if get_domain_item(s) then
        return {["value"]=s}
      end
    end
  },
  ["^https?://([^/]+/cdn%-cgi/.+)$"]={
    ["type"]="asset",
    ["additional"]="^https?://([^/]+/%.shared/.+)$"
  },
  ["^https?://([^/]+/.+)$"]={
    ["type"]="article",
    ["additional"]=function(s)
      if not string.match(s, "%.html$")
        and not string.match(s, "%.html[^0-9a-zA-Z]")
        and not string.match(s, "/$") then
        return nil
      end
      if string.match(s, "[^/]+/%.a/")
        or (
          string.match(s, "[^/]+/%.shared/")
          and not string.match(s, "^[^/]+/%.shared/image%.html")
        )
        or string.match(s, "[^/]+/cdn%-cgi/") then
        return nil
      end
      for _, pattern in pairs({
        "^(.-/)page/[0-9]+/$",
        "^(.-/)comment%-page%-[0-9]+/$",
      }) do
        local temp = string.match(s, pattern)
        if temp then
          s = temp
          break
        end
      end
      local blog = get_domain_item(s)
      if not blog or blog == "profile" then
        return nil
      end
      local path = string.match(s, "^[^/]+/(.+)$")
      if not path then
        return nil
      end
      return {
        ["value"]=blog .. ":" .. path,
        ["path"]=path,
        ["extra_ids"]={string.lower(path)},
        ["blog"]=blog
      }
    end
  }
}
for pattern, data in pairs(item_patterns) do
  if type(data) == "string" then
    data = {["type"]=data}
  end
  if not data["additional"] then
    data["additional"] = function(s) return {["value"]=s} end
  end
  if type(data["additional"]) == "string" then
    data["additional"] = item_patterns[data["additional"]]["additional"]
    if not data["additional"] then
      error("Could not initialize item patterns.")
    end
  end
  item_patterns[pattern] = data
end

extraction_patterns = {
  ["^https?://([^/]+)/"]=item_patterns["^https?://([^/]+)/$"],
  ["^https?://profile%.typepad%.com/([0-9a-zA-Z]+)"]=item_patterns["^https?://profile%.typepad%.com/([0-9a-zA-Z]+)$"],
  ["^https?://([^/]+)"]={
    ["type"]="maybeblog",
    ["additional"]=function(s)
      if string.match(s, "^[^/%.]+%.typepad%.com$")
        or s == context["blog"] then
        return nil
      end
      return {["value"]=s}
    end
  },
}
for k, v in pairs(item_patterns) do
  extraction_patterns[k] = v
end
for _, pattern in pairs({
  "^https?://([^/]+)/$",
  "^https?://profile%.typepad%.com/([0-9a-zA-Z]+)$"
}) do
  if not extraction_patterns[pattern] then
    error("Could not find pattern.")
  end
  extraction_patterns[pattern] = nil
end

get_item_data = function(url, pattern, pattern_data)
  local value = string.match(url, pattern)
  if not value then
    return nil
  end
  local data = pattern_data["additional"](value)
  if data then
    if not data["type"] then
      data["type"] = pattern_data["type"]
    end
    return data
  end
end

find_item = function(url)
  if ids[url] then
    return nil
  end
  for pattern, data in pairs(item_patterns) do
    local data = get_item_data(url, pattern, data)
    if data then
      return data
    end
  end
end

set_item = function(url)
  found = find_item(url)
  if found then
    local newcontext = {}
    if (found["type"] == "asset" or found["type"] == "asset2")
      and assets[url] then
      found["type"] = assets[url]
    end
    new_item_type = found["type"]
    new_item_value = found["value"]
    for k, v in pairs(found) do
      if k ~= "type" and k ~= "value" then
        newcontext[k] = v
      end
    end
    new_item_name = new_item_type .. ":" .. new_item_value
    if new_item_name ~= item_name
      and (
        not found["image_id"]
        or found["image_id"] ~= context["image_id"]
      ) then
      ids = {}
      context = newcontext
      item_value = new_item_value
      item_type = new_item_type
      ids[string.lower(item_value)] = true
      if context["extra_ids"] then
        for _, extra_id in pairs(context["extra_ids"]) do
          ids[extra_id] = true
        end
      end
      abortgrab = false
      tries = 0
      retry_url = false
      is_initial_url = true
      item_name = new_item_name
      print("Archiving item " .. item_name)
    end
  end
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

allowed = function(url, parenturl)
  local noscheme = string.match(url, "^https?://(.*)$")

  if ids[url]
    or (noscheme and ids[string.lower(noscheme)]) then
    return true
  end

  if string.match(url, "'%s*%+%s*'")
    or string.match(url, "/%*")
    or string.match(url, "/svc=blogs/blog_id=")
    or string.match(url, "^https?://[^/]+/sitelogin")
    or string.match(url, "^https?://[^/]+/sitelogout")
    or string.match(url, "^https?://[^/]+/%.services/sitelogin")
    or string.match(url, "^https?://[^/]+/%.services/sitelogout")
    or string.match(url, "^https?://[^/]+/%.?services/connect/profile_module")
    or string.match(url, "%?no_prefetch=1$")
    or string.match(url, "%?cid=")
    or (
      (
        item_type == "blog"
        or item_type == "article"
      )
      and parenturl
      and string.match(parenturl, "^(.-/page/[0-9]+/)$") == string.match(url, "^(.-/page/[0-9]+/)")
      and string.match(url, "^.-/page/[0-9]+/.")
    )
    or (
      item_type == "profile"
      and (
        string.match(url, "/events%?start_token=$")
        or string.match(url, "/activity/atom%.xml$")
      )
    ) then
    return false
  end

  local skip = false
  for pattern, data in pairs(extraction_patterns) do
    match = get_item_data(url, pattern, data)
    if match then
      local new_item = match["type"] .. ":" .. match["value"]
      local to_skip = match["type"] ~= "blog" and match["type"] ~= "maybeblog"
      if new_item ~= item_name then
        if match["type"] == "article" then
          local dir = string.match(url, "^(https?://[^/]+/.+/).")
          if dir then
            allowed(dir, parenturl)
          end
        end
        discover_item(discovered_items, new_item)
      elseif to_skip then
        return true
      end
      if to_skip then
        skip = true
      end
    end
  end
  if skip then
    return false
  end

  if not get_domain_item(url) then
    discover_item(discovered_outlinks, string.match(percent_encode_url(url), "^([^%s]+)"))
    return false
  end

  if get_domain_item(url) then
    local path = string.match(url, "^https?://[^/]+/(.*)$")
    if path and ids[string.lower(path)] then
      return true
    end
    for _, pattern in pairs({
      "([0-9a-zA-Z_]+)",
      "([^%./]+)",
      "([^/]+)"
    }) do
      for s in string.gmatch(url, pattern) do
        if ids[string.lower(s)] then
          return true
        end
      end
    end
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

  --[[if allowed(url, parent["url"])
    and not processed(url)
    and string.match(url, "^https://")
    and not addedtolist[url] then
    addedtolist[url] = true
    return true
  end]]

  return false
end

decode_codepoint = function(newurl)
  newurl = string.gsub(
    newurl, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])",
    function (s)
      return utf8.char(tonumber(s, 16))
    end
  )
  return newurl
end

percent_encode_url = function(newurl)
  result = string.gsub(
    newurl, "(.)",
    function (s)
      local b = string.byte(s)
      if b < 32 or b > 126 then
        return string.format("%%%02X", b)
      end
      return s
    end
  )
  return result
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  local json = nil

  downloaded[url] = true

  if abortgrab then
    return {}
  end

  local function fix_case(newurl)
    if not newurl then
      newurl = ""
    end
    if not string.match(newurl, "^https?://[^/]") then
      return newurl
    end
    if string.match(newurl, "^https?://[^/]+$") then
      newurl = newurl .. "/"
    end
    local a, b = string.match(newurl, "^(https?://[^/]+/)(.*)$")
    return string.lower(a) .. b
  end

  local function check(newurl)
    if not string.match(newurl, "^https?://") then
      return nil
    end
    if string.match(newurl, "[\r\n]") then
      for new in string.gmatch(newurl, "([^\r\n]+)") do
        check(new)
      end
      return nil
    end
    local post_body = nil
    local post_url = nil
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    newurl = fix_case(newurl)
    local origurl = url
    if string.len(url) == 0
      or string.len(newurl) == 0 then
      return nil
    end
    local url = string.match(newurl, "^([^#]+)")
    local url_ = string.match(url, "^(.-)[%.\\]*$")
    while string.find(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    if string.match(newurl, "/index%.html") then
      check(string.match(newurl, "^(.+/)[^/]+$"))
    end
    if string.match(newurl, ",") then
      for s in string.gmatch(newurl, "([^,]+)") do
        check(urlparse.absolute(newurl, s))
      end
    end
    if string.match(newurl, "image%.html%?.") then
      check(urlparse.absolute(newurl, string.match(newurl, "%?(.+)$")))
    end
    if string.match(newurl, "%?no_prefetch=1$") then
      check(string.match(newurl, "^(.+)%?"))
    end
    if not processed(url_)
      and not processed(url_ .. "/")
      and allowed(url_, origurl) then
      local headers = {}
      table.insert(urls, {
        url=url_,
        headers=headers
      })
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "['\"><]") then
      return nil
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (
      string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^data:")
      or string.match(newurl, "^irc:")
      or string.match(newurl, "^%${")
    ) then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function set_new_params(newurl, data)
    for param, value in pairs(data) do
      if value == nil then
        value = ""
      elseif type(value) == "string" then
        value = "=" .. value
      end
      if string.match(newurl, "[%?&]" .. param .. "[=&]") then
        newurl = string.gsub(newurl, "([%?&]" .. param .. ")=?[^%?&;]*", "%1" .. value)
      else
        if string.match(newurl, "%?") then
          newurl = newurl .. "&"
        else
          newurl = newurl .. "?"
        end
        newurl = newurl .. param .. value
      end
    end
    return newurl
  end

  local function increment_param(newurl, param, default, step)
    local value = string.match(newurl, "[%?&]" .. param .. "=([0-9]+)")
    if value then
      value = tonumber(value)
      value = value + step
      return set_new_params(newurl, {[param]=tostring(value)})
    else
      if default ~= nil then
        default = tostring(default)
      end
      return set_new_params(newurl, {[param]=default})
    end
  end

  local function get_count(data)
    local count = 0
    for _ in pairs(data) do
      count = count + 1
    end
    return count
  end

  if item_type == "asset2" then
    if status_code == 200 then
      local server, rest = string.match(url, "^https?://([a-z]+)[0-9]?(%.typepad%.com/.+)")
      if (server == "a" or server == "up") and rest then
        for i = 1 , 7 do
          check("https://" .. server .. tostring(i) .. rest)
        end
      end
    end
    if string.match(item_value, "/[0-9a-f]+$") then
      check(urlparse.absolute(url, string.match(item_value, "^[^/]+(/.+)$") .. "-pi"))
      check(urlparse.absolute(url, string.match(item_value, "^[^/]+(/.+)$")))
    end
    if string.match(url, "/[0-9a-f]+$") then
      check(url .. "-pi")
    end
  end

  if item_type == "asset" then
    local base = string.match(url, "^(https?://[^/]+/%.a/[0-9a-f]+)")
    if not base then
      base = string.match(url, "^(https?://a[0-9]%.typepad%.com/[0-9a-f]+)")
    end
    if base then
      check(base)
      for _, s in pairs({
        "pi",
        "popup"
      }) do
        check(base .. "-" .. s)
      end
      for _, stype in pairs({"pi", "wi", "si"}) do
        for _, n in pairs(sizes[stype]) do
          check(base .. "-" .. tostring(n) .. stype)
        end
      end
    end
    local server, image_id = string.match(url, "^https?://([a-z]+)[0-9]?%.typepad%.com/([0-9a-f]+)")
    if (server == "a" or server == "up") and image_id then
      for i = 1 , 7 do
        check("https://" .. server .. tostring(i) .. ".typepad.com/" .. image_id)
      end
    end
    if server == "up" then
      for _, s in pairs({
        "pi",
      }) do
        check(urlparse.absolute(url, "/" .. image_id .. "-" .. s))
      end
      for _, n in pairs(sizes["si"]) do
        check(urlparse.absolute(url, "/" .. image_id .. "-" .. tostring(n) .. "si"))
      end
    end
  end

  local function extract_variable(s)
    for name, pattern in pairs({
      ["tpconnect"] = "TPConnect%.([0-9a-z_]+)",
      ["encode"] = "encodeURIComponent%(([0-9a-z_]+)%)",
      ["string"] = "'([^']*)'",
      ["number"] = "(%-?[0-9]+)",
      ["date"] = "(tpe_date%.getTime%(%))"
    }) do
      pattern = "^%s*" .. pattern .. "%s*;?%s*$"
      local value = string.match(s, pattern)
      if value then
        if name == "tpconnect" then
          return name, context["tpconnect"][value]
        elseif name == "encode" then
          return name, value
        elseif name == "string" then
          return name, value
        elseif name == "number" then
          return name, tonumber(value)
        elseif name == "date" then
          return name, os.time() * 1000
        else
          error("Should not reach this.")
        end
      end
    end
    error("Could not extract variable.")
  end

  if allowed(url)
    and status_code < 300
    and (
      (item_type ~= "asset" and item_type ~= "asset2")
      or string.match(url, "%-popup$")
    )
    and not string.match(url, "%.jpg$")
    and not string.match(url, "%.png$")
    and not string.match(url, "%.gif$") then
    html = read_file(file)
    if item_type == "blog"
      and string.match(url, "^https?://[^/]+/$") then
      local user_id = string.match(html, "user_id=([0-9]+)")
      if user_id then
        discover_item(discovered_items, "userid:" .. user_id)
      end
      local profile_s = string.match(html, "profile_module[^\"]*[%?&]user_id=([^&\"]+)")
      if profile_s then
        check("https://profile.typepad.com/" .. profile_s)
      end
      for path, patterns in pairs({
        ["/t/rsd/"]={
          "%?blog_id=([0-9]+)"
        },
        ["/services/rsd/"]={
          "/%.services/blog/([0-9a-f]+)",
          "ga%('Typepad%.set', 'dimension1', '([0-9a-f]+)'%);",
          "/services/rsd/([0-9a-f]+)",
          "/t/rsd/([0-9a-f]+)"
        }
      }) do
        local found = false
        for _, pattern in pairs(patterns) do
          local blog_id = string.match(html, pattern)
          if blog_id then
            ids[blog_id] = true
            check("https://www.typepad.com" .. path .. blog_id)
          end
          found = true
        end
        if not found then
          error("Could not find a blog ID.")
        end
      end
    end

    if item_type == "article"
      and string.match(url, "%.html$") then
      if not string.match(html, "comments to this entry are closed")
        and not string.match(html, "/embed%.js%?asset_id=")
        and not string.match(html, "for=\"jp%-carousel%-comment%-form%-author%-field\"")
        and not string.match(html, "<input type=\"submit\" name=\"post\" id=\"comment%-post%-button\"")
        and not string.match(html, "<p class=\"comments%-closed")
        and not string.match(html, "action=\"https://www%.typepad%.com/t/comments\" name=\"comments_form\"")
        and string.match(string.gsub(html, "<h4><a id=\"comments\"></a>Comments</h4>", ""), "[cC]omments")
        and not string.match(url, "^https?://[^/]+/photos/[^/]+/[^%./]+%.html$")
        and not string.match(url, "/20[012][0-9]/[01][0-9]/index%.html$")
        and string.match(html, "[cC]omments") then
        error("Unsupported comments methods found.")
      end
      local profile_s = string.match(html, "profile_module[^\"]*[%?&]user_id=([^&\"]+)")
      if profile_s then
        check("https://profile.typepad.com/" .. profile_s)
      end
      local tpc_title = string.match(html, "<div[^>]+id=\"tpc_post_title\">(.-)</div>")
      local tpc_message = string.match(html, "<div[^>]+id=\"tpc_post_message\">(.-)</div>%s*<script")
      context["tpconnect"] = {}
      for k, v in string.gmatch(html, "TPConnect%.([a-z0-9_]+)%s*=%s*(.-)%s*;%s") do
        local temp = string.match(v, "^'(.*)'$")
        if temp then
          v = temp
        else
          inner_k = string.match(v, "document%.getElementById%('([^']+)'%)%.innerHTML")
          if not inner_k then
            error("Could not extract TPConnect data.")
          end
          if inner_k == "tpc_post_title" then
            v = tpc_title
          elseif inner_k == "tpc_post_message" then
            v = tpc_message
          else
            error("Unknown PTConnect key.")
          end
        end
        --[[v = string.gsub(v, '(&[^&;]+;)', function(s)
          local decoded = html_entities.decode(s)
          if decoded == s then
            return s
          end
          local encoded = ({
            ["&#0160;"]="&nbsp;",
            ["&#39;"]="&#39;"
          })[s]
          if not encoded then
            error("Found unsupported encoded character " .. s .. ".")
          end
          return encoded
        end)]]
        context["tpconnect"][k] = string.gsub(v, "\r\n", "\n")
      end
      for asset_id in string.gmatch(html, "/embed%.js%?asset_id=([0-9a-f]+)") do
        ids[asset_id] = true
      end
    end

    if string.match(url, "/thread%.js%?asset_id=")
      and (
        string.match(html, "Show more comments")
        or string.match(html, "TPConnect%.blogside%.appendPage%(TPConnect%.lowest_comment, TPConnect%.newest_comment%)")
      ) then
      if not string.match(html, "TPConnect%.blogside%.appendPage%(TPConnect%.lowest_comment, TPConnect%.newest_comment%)") then
        error("No data found on more comments.")
      end
      local comments_data = {
        ["posted_comment"]="",
        ["lowest_comment"]="",
        ["newest_comment"]=""
      }
      for k, v in string.gmatch(html, "TPConnect%.([a-z]+_comment)%s*=%s*'([0-9a-f]+)'") do
        comments_data[k] = v
      end
      local newurl = context["tpconnect"]["embed_src"]
        .. '?asset_id=' .. context["tpconnect"]["post_xid"]
        .. '&d=1'
        .. '&p=1'
        .. '&dc=0'
        .. '&posted_comment=' .. comments_data["posted_comment"]
        .. '&lowest_comment=' .. comments_data["lowest_comment"]
        .. '&newest_comment=' .. comments_data["newest_comment"]
        .. '&ts='
      check(urlparse.absolute(url, newurl))
    end

    if string.match(url, "/embed%.js%?asset_id=") then
      for k, v in string.gmatch(html, "TPConnect%.([a-z0-9_]+)%s*=%s*'(.-)'%s*;%s") do
        context["tpconnect"][k] = v
      end

      local message = context["tpconnect"]["tpc_message"]
      local max_message_length = tonumber(string.match(html, "if%(tpe_message%.length > ([0-9]+)%)"))
      assert(max_message_length>0)
      if utf8.len(message) > max_message_length then
        local start_index = tonumber(string.match(html, "tpe_message%.indexOf%(' ', ([0-9]+)%)%);"))
        assert(start_index>0)
        local index = utf8.find(message, " ", start_index+1, true)
        if index then
          message = utf8.sub(message, 1, index-1)
        end
      end
      if utf8.len(message) > max_message_length then
        message = utf8.sub(message, 1, max_message_length)
      end
      context["tpconnect"]["tpc_message_original"] = context["tpconnect"]["tpc_message"]
      context["tpconnect"]["tpc_message"] = message

      local make_url_data = string.match(html, "tpe_script%.src%s*=%s*(TPConnect%.embed_src.-;)")
      local newurls = {""}
      local expect_end = false
      local prev_value = ""
      for part in string.gmatch(make_url_data, "([^%+]+)") do
        expect_end = string.match(part, ";%s*$")
        local found = false
        local name, value = extract_variable(part)
        if name == "encode" then
          local inner = string.match(html, "%s" .. value .. "%s*=%s*([^;]+);")
          local inner_name, inner_value = extract_variable(inner)
          if inner_name == "encode" then
            error("Did not expect encode variable.")
          end
          value = urlparse.escape(inner_value)
        end
        local extra_newurls = {}
        if prev_value == "&ts=" or prev_value == "&message=" then
          value = ""
        end
        for i, newurl in pairs(newurls) do
          if string.match(prev_value, "[%?&]message=$")
            or string.match(prev_value, "[%?&]ts=$") then
            table.insert(extra_newurls, newurl)
          elseif string.match(prev_value, "[%?&]asset_id=$") then
            ids[value] = true
          end
          newurls[i] = newurl .. value
        end
        for _, newurl in pairs(extra_newurls) do
          table.insert(newurls, newurl)
        end
        prev_value = value
      end
      if not expect_end then
        error("Did not expect end to URL building.")
      end
      for _, newurl in pairs(newurls) do
        check(urlparse.absolute(url, newurl))
      end
      context["tpconnect"]["tpc_message"] = context["tpconnect"]["tpc_message_original"]

      local moreurl = string.match(html, "src=\"([^\"']+)'%s*%+%s*moreurl%s*%+%s*'\"")
      if moreurl then
        moreurl = moreurl .. "&color=" .. urlparse.escape("#444444") .. "&width=764"
        if string.match(moreurl, "moreurl") then
          error("Moreurl not correctly replaced.")
        end
        check(moreurl)
      end
    end
    html = "text" .. html
    for newurl in string.gmatch(string.gsub(html, "&[qQ][uU][oO][tT];", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, "url%(([^%)]+)%)") do
      checknewurl(newurl)
    end
    html = string.gsub(html, "&gt;", ">")
    html = string.gsub(html, "&lt;", "<")
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
  end

  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  status_code = http_stat["statcode"]
  set_item(url["url"])
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()
  logged_response = true
  if not item_name then
    error("No item name found.")
  end
  is_initial_url = false
  if http_stat["statcode"] == 500
    and item_type == "asset"
    and (
      string.match(url["url"], "%-[0-9]+[a-z][a-z]$")
      or string.match(url["url"], "%-popup$")
    ) then
    discover_item(discovered_items, "asset500:" .. string.match(url["url"], "^https?://(.+)$"))
    retry_url = false
    tries = 0
    return false
  end
  if http_stat["statcode"] ~= 200
    and http_stat["statcode"] ~= 301
    and http_stat["statcode"] ~= 302
    and http_stat["statcode"] ~= 404
    and (
      not string.match(url["url"], "^https?://[^/]+/t/rsd/")
      or http_stat["statcode"] ~= 500
    ) then
    retry_url = true
    return false
  end
  if http_stat["len"] == 0
    and http_stat["statcode"] < 300 then
    retry_url = true
    return false
  end
  if abortgrab then
    print("Not writing to WARC.")
    return false
  end
  retry_url = false
  tries = 0
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]

  if not logged_response then
    url_count = url_count + 1
    io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
    io.stdout:flush()
  end
  logged_response = false

  if killgrab then
    return wget.actions.ABORT
  end

  set_item(url["url"])
  if not item_name then
    error("No item name found.")
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  if status_code == 0 or retry_url then
    io.stdout:write("Server returned bad response. ")
    io.stdout:flush()
    tries = tries + 1
    local maxtries = 3
    if status_code == 503 then
      maxtries = 10
    elseif status_code == 403 then
      maxtries = 20
    end
    if tries > maxtries then
      io.stdout:write(" Skipping.\n")
      io.stdout:flush()
      tries = 0
      abort_item()
      return wget.actions.EXIT
    end
    local factor = 1.1
    if status_code == 503 then
      factor = 2
    end
    local sleep_time = math.random(
      math.floor(math.pow(factor, tries-0.5)),
      math.floor(math.pow(factor, tries))
    )
    io.stdout:write("Sleeping " .. sleep_time .. " seconds.\n")
    io.stdout:flush()
    os.execute("sleep " .. sleep_time)
    return wget.actions.CONTINUE
  else
    if status_code == 200 then
      if not seen_200[url["url"]] then
        seen_200[url["url"]] = 0
      end
      seen_200[url["url"]] = seen_200[url["url"]] + 1
    end
    downloaded[url["url"]] = true
  end

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if not allowed(newloc, url["url"]) or processed(newloc) then
      tries = 0
      return wget.actions.EXIT
    end
  end

  tries = 0

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local function submit_backfeed(items, key)
    local tries = 0
    local maxtries = 5
    while tries < maxtries do
      if killgrab then
        return false
      end
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key,
        items .. "\0"
      )
      if code == 200 and body ~= nil and cjson.decode(body)["status_code"] == 200 then
        io.stdout:write(string.match(body, "^(.-)%s*$") .. "\n")
        io.stdout:flush()
        return nil
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    kill_grab()
    error()
  end

  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-items.txt", "w")
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
  for key, data in pairs({
    ["typepad-twsgkwlnvokoy991"] = discovered_items,
    ["typepad-extra-2t83qhi02ad58rm6"] = discovered_extra_items,
    ["urls-9laax4qga25pjo8y"] = discovered_outlinks
  }) do
    print("queuing for", string.match(key, "^(.+)%-"))
    local items = nil
    local count = 0
    for item, _ in pairs(data) do
      --print("found item", item)
      if items == nil then
        items = item
      else
        items = items .. "\0" .. item
      end
      count = count + 1
      if count == 1000 then
        submit_backfeed(items, key)
        items = nil
        count = 0
      end
    end
    if items ~= nil then
      submit_backfeed(items, key)
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    abort_item()
  end
  return exit_status
end


