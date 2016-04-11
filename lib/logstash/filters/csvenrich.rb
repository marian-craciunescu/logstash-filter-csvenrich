# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"
require "csv"

# The cvslookup filter allows you to add fields to an event
# base on a csv file

class LogStash::Filters::CSVEnrich < LogStash::Filters::Base
  config_name "csvenrich"

  # Example:
  #
  #    filter {
  #       csvlookup {
  #        file => 'key_value.csv'
  #        key_col => "id"
  #        lookup_col => "userid"
  #        map_field => { "name" => "[user][name]" }
  #      }
  #    }  
  #       
  config :file, :validate => :string, :required => true
  config :key_col, :validate => :string, :default => "id", :required => true
  config :lookup_col, :validate => :string, :default => "id", :required => true
  config :map_field, :validate => :hash, :default => Hash.new, :required => false
  config :refresh_interval, :validate => :number, :default => 300

  public
  def register
 
    if @file
      if File.zero?(@file)
        raise "file is empty"
      end
      @next_refresh = Time.now + @refresh_interval
      raise_exception = true
      reload(raise_exception)
    end

    #puts @lookup.inspect
  end # def register

  public
  def reload(raise_exception=false)
     @lookup = Hash.new
     CSV.foreach(@file, headers: true) do |row|
       key = row[@key_col]
       if !key.nil?
         @lookup[key] = row.to_hash
       end
     end
  end

  public
  def filter(event)
   if @file
      if @next_refresh < Time.now
        reload
        @next_refresh = Time.now + @refresh_interval
        print "reloading csv"
        @logger.debug? and @logger.debug("Reloading csv")
      end
    end

    return unless filter?(event)
    event_lookup_val = event[@lookup_col]
    if !event_lookup_val.nil?
        looked_up_value = @lookup[event_lookup_val]
	if !looked_up_value.nil?
	  @map_field.each do |src_field, dest_field|
	    val = looked_up_value[src_field]
            if !val.nil?
              event[dest_field] = val
            end
	  end
	  filter_matched(event)
       end
    end
  end # def filter
end # class LogStash::Filters::CSVLookup
