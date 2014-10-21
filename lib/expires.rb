require "expires/version"
require 'sqlite3'
require 'json'
require 'pry'

class Expires
  class DummyIO;def puts str;end;end
  
  KEYS = %w[key value created_at updated_at expire on_expire remind]
  SCHEMA = %Q(create table NAMESPACE (key TEXT PRIMARY KEY, value TEXT, created_at INTEGER, updated_at INTEGER, expire INTEGER, on_expire TEXT, remind TEXT);)

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
    @remind     = Hash.new

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
  
  def schema namespace
    return SCHEMA.gsub("NAMESPACE",namespace)
  end

  def create(namespace)
    begin
      @@db.execute(sql = schema(namespace))
      @@io.puts sql
    rescue SQLite3::SQLException => e
      raise e unless e.message =~ /table .* already exists/
    end
    return namespace
  end

  def kill_expired
    @killing = true
    now = Time.new.to_i
    expires = @@db.execute(sql = "select on_expire, key, value from #{@namespace} where (updated_at+expire) < #{now};")
    expires.each do |values|
      if values[0]
        begin
          @key = values[1]
          @value = values[2]
          flash(@key) 
          @self = self
          eval values[0].gsub("$n","\n")
        rescue =>e
          STDERR.puts e
          STDERR.puts e.backtrace
        end
      end
    end

    @@db.execute(sql = "delete from #{@namespace} where (updated_at+expire) < #{now};")
    @@io.puts sql
    @killing = nil
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

  def set key, value, expire = @expire
    @body[key] = value
    record = select(key, :created_at)
    
    if record
      created_at = record[0]
      updated_at = Time.new.to_i
    else
      updated_at = created_at = Time.new.to_i
    end
    
    @@db.execute(sql="insert or replace into #{@namespace} (key,value,created_at,updated_at,expire)"+
                 " values ('#{key}','#{value}','#{created_at}','#{updated_at}','#{expire}'#{});")
    @@io.puts sql
    #TODO: syncedで状態管理
  end

  def []=(key,value)
    set(key,value)
  end

  def clean
    @@db.execute("delete from #{@namespace};")
    @@db.execute("VACUUM;")
  end

  def remind key, hash
    raise "Remind your value as Hash." unless hash.is_a?(Hash)
    update key, :remind, JSON.generate(hash) if get key
    @remind[key.to_sym] = hash
  end

  def forget key
    raise "Remind your value as Hash." unless hash.is_a?(Hash)
    update key, :remind, "{}" if get key
    @remind[key.to_sym] = {}
  end

  def flash key
    if value = select(key, :remind)
      begin
        @remind = JSON.parse value.first
      rescue
        STDERR.puts "Invalid JSON value."
      end
    end
  end

  def select key, *columns
    kill_expired unless @killing
    return @body[key.to_sym] = @@db.execute("select #{columns.join(", ")} from #{@namespace} where key = '#{key}' limit 1;").first
  end

  def get key
    return select(key, :value).first
  rescue => e
    return nil
  end

  def [](key)
    get key
  end

  def update key, column, value
    sql = Expires.escape_sqlite3("update :_namespace_ set #{column} = (:#{column}) where key = (:key);",
                                 column=>value,
                                 :key=>key, 
                                 :_namespace_=>@namespace)
    @@db.execute(sql)
    sql
  end

  def updates key, hash
    sql = "update :_namespace_ set "
    first = true
    hash.each do |k, v|
      if first
        first = false
      else
        sql << ','
      end
      sql << "#{k} = :#{k} "
    end
    sql << "where key = (:key);"

    sql = Expires.escape_sqlite3(sql,
                                 :key=>key, 
                                 :_namespace_=>@namespace,
                                 **hash)
    @@db.execute(sql)
    sql
  end
  
  def closer key, procedure=nil, remind: nil, &block
    if block_given?
      obj = Object.new
      obj.define_singleton_method(:_, &block)
      procedure = obj.method(:_).to_proc
    end

    source = Expires.get_source_str(procedure.source).gsub(/\n/,"$n")
    if get key
      sql = updates key, {:on_expire=>source.to_s, :remind=>JSON.generate(remind||{})}
    else
      #procedure.call
      return
    end

    kill_expired
    @@io.puts sql
  end
  
  def self.get_source_str source
    source.gsub!(/\n/,"$n")
    source.gsub!(/\A.*?(-> +\(.*\)|lambda) +?(do|\{)( +)?(\|.*?\|)?/, "")
    source.gsub!(/\A.*?closer.*?(do|\{)( +)?(\|.*?\|)?/, "")
    source.reverse!
    source.gsub!(/\A(.*?)(dne|\})/, "")
    source.reverse!
    source.gsub!("$n","\n")
    return source
  end

  def self.escape_sqlite3(string, **hash)
    hash.each do |key, value|
      value.gsub!(/'/,"''") if value.is_a?(String)
      value = "null" if value.nil?
      string.gsub!(":#{key}", "'#{value}'")
    end
    return string
  end
end

def Expires.new namespace: namespace, expire: 5, hotload: true
  raise "namespace is nil." if namespace.nil? or namespace == ""
  return Expires.db(namespace) || super
end
