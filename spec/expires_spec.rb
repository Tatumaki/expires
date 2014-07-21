require 'spec_helper'

describe Expires do
  it 'has a version number' do
    expect(Expires::VERSION).not_to be nil
  end

  context "on each connection state" do
    describe "#database" do
      it 'returns scpecific string' do 
        Expires.database = 'spec_test'
        expect( Expires.database ).to eq "spec_test"
      end
    end
  end

  context "on initialize" do
    describe "when hotload is true" do
      expires = Expires.new(namespace: "on_initialize_when_hotload_is_true")
      expires["a"] = :a
      expires["b"] = :b
      expires.disconnect

      expires = Expires.new(namespace: "on_initialize_when_hotload_is_true", hotload: true)
      it "have to loaded 'a' as 'a'" do
        expect(expires["a"]).to eq 'a'
      end
      
      it "have to loaded 'b' as 'a'" do
        expect(expires["b"]).to eq 'b'
      end
    end

    describe "when hotload is false" do
      expires = Expires.new(namespace: "on_initialize_when_hotload_is_false")
      expires["a"] = :a
      expires["b"] = :b
      expires.disconnect

      expires = Expires.new(namespace: "on_initialize_when_hotload_is_false", hotload: false)
      it "have to loaded 'a' as nil" do
        expect(expires["a"]).to eq nil
      end
      
      it "have to loaded 'b' as nil" do
        expect(expires["b"]).to eq nil
      end
    end
  end

  context "when not connected" do
  end

  context "when connected" do
  end



=begin
  def self.db(namespace)
    @@dbs[namespace]
  end

  def initialize namespace: namespace, expire: 5, hotload: true
    @@db        = SQLite3::Database.new(@@database) unless @@db
    @namespace  = namespace
    @connected  = false
    @expire     = expire

    connect(namespace)
    sync
  end
  
  def sync(async: false)
    raise "Database not connected" unless @connected
    kill_expired
    @@db.execute("select key,value from #{@namespace};").each do |key,value|
      @body[key] = value
    end
  end
  
  def create(namespace)
    begin
      @@db.execute(sql = "create table #{namespace} (key TEXT PRIMARY KEY, value TEXT, created_at INTEGER, updated_at INTEGER, expire INTEGER);")
      @@io.puts sql
    rescue SQLite3::SQLException => e
      raise e unless e.message == 'table test already exists'
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
      @@db.execute("select key,value from #{namespace};").each do |row|
        @body[row[0]] = row[1]
      end
      @@dbs.store(namespace, self)
      @synced = true
      @connected = true
    end
  end

  def disconnect
    @@db.close if @@db
    @connected = false
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
    self.kill_expired
    return @body[key] = @@db.execute("select value from #{@namespace} where key = '#{key}' limit 1;").first
  rescue => e
    puts e
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

=end



end
