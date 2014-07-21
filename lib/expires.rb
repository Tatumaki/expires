require "expires/version"
require 'sqlite3'

class Expires
  class DummyIO;def puts str;end;end

  @@dbs = Hash.new #namespace: instance
  @@database = 'expires.sqlite3'
  @@db = nil # Main instance
  @@io = DummyIO.new
  
  def self.database=(path)
    @@database = path
  end

  def self.database
    @@database
  end

  def self.db(namespace)
    @@dbs[namespace]
  end

  def initialize namespace: namespace, expire: 5, hotload: true
    @@db        = SQLite3::Database.new(@@database) unless @@db
    @namespace  = namespace
    @connected  = false
    @expire     = expire
    @hotload    = hotload
    @body       = Hash.new

    connect(namespace)
    sync if @hotload
  end
  
  def sync(async: false)
    raise "Database not connected" unless @connected
    kill_expired
    @@db.execute("select key,value from #{@namespace};").each do |array|
      @body[array[0]] = array[1]
    end
    @synced = true
  end
  
  def create(namespace)
    begin
      @@db.execute(sql = "create table #{namespace} (key TEXT PRIMARY KEY, value TEXT, created_at INTEGER, updated_at INTEGER, expire INTEGER);")
      @@io.puts sql
    rescue SQLite3::SQLException => e
      raise e unless e.message =~ /table .* already exists/
    end
    return namespace
  end

  def kill_expired
    now = Time.new.to_i
    @@db.execute(sql = "delete from #{@namespace} where (updated_at+expire) < #{now};")
    @@io.puts sql
  end

  def connect(namespace)
    unless @@dbs[namespace]
      create(namespace)
      @body = Hash.new
      @@dbs.store(namespace, self)
      @connected = true
    end
  end

  def disconnect
    if @connected
      @@dbs.delete @namespace
      @body = nil
      @connected = false
    end
  end

  def set key, value
    @body[key] = value
    kill_expired
    record = @@db.execute("select created_at from #{@namespace} where key = '#{key}' limit 1;").first
    
    if record
      created_at = record[0]
      updated_at = Time.new.to_i
    else
      updated_at = created_at = Time.new.to_i
    end

    @@db.execute(sql="insert or replace into #{@namespace} (key,value,created_at,updated_at,expire) values ('#{key}','#{value}','#{created_at}','#{updated_at}','#{@expire}');")
    @@io.puts sql
    #TODO: syncedで状態管理
  end

  def [](key)
    if @hotload
      self.kill_expired
      return @body[key] = @@db.execute("select value from #{@namespace} where key = '#{key}' limit 1;").first.first
    else
      return @body[key]
    end
  rescue => e
    return nil
  end

  def []=(key,value)
    set(key,value)
  end
end

def Expires.new namespace: namespace, expire: 5, hotload: true
  raise "namespace is nil." if namespace.nil? or namespace == ""
  return Expires.db(namespace) || super
end
