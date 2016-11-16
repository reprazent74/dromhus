#!/usr/bin/ruby

require 'httparty'
require 'nokogiri'
require 'sqlite3'
require 'optparse'

# Get the contents of a file
def get_file( file )
  file_contents = ""
  File::open(file, "r") { |f| file_contents = f.read }
  file_contents
end

# Writes a string to file
def write_file( file, contents )
  File::open( file, "w" ) do |f|
    f.write( contents )
  end
end

# Writes to file only if the contents are changed
def write_only_changes_to_file( file, contents )
  if (File.exist?(file))
    write_file(file, contents) if (contents != get_file(file))
  else
    write_file(file, contents)
  end
end

class Log

	@@log_file = "log.txt"

	def initialize
		write_file(@@log_file, "") unless (File.exist?(@@log_file))
	end

	def self.write(str)
		timestamp = Time.now.to_s.split(" ").slice(0,2).join(" ").ljust(25)
		open(@@log_file, 'a') { |f| f.puts timestamp + str }
	end
end

class String
	def clean_digits
		self.gsub(/\D/, "")
	end
end

def get_html(link, do_hard_fetch=false)
	file_name = "data/" + link.split('/')[-1] + ".html"
	if (File.exist?(file_name) && !do_hard_fetch)
		return Nokogiri::HTML(get_file(file_name))
	else
		Log.write "Fetching from web '#{link}'"
		contents = Nokogiri::HTML(HTTParty.get("http://www.hemnet.se" + link))
		write_only_changes_to_file(file_name, contents)
		sleep(1)
		return contents
	end
end

def now
	t = Time.now
	"#{t.year}-#{t.mon}-#{t.day} #{t.hour}:00"
end

module Database

	@db = File.exists?( "data.sqlite" ) ? SQLite3::Database.open( "data.sqlite" ) : SQLite3::Database.new( "data.sqlite" )

	def Database.db_create_table(name, fields)
		if (@db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='#{name}'").empty? == true)
			@db.execute( "CREATE TABLE #{name}(#{fields})" )
			Log.write "Table: '#{name}' created"
		end
	end

	def Database.db_create_tables
		# 3 tables are needed: objects, datapoints, viewings
		Database.db_create_table("objects", "id INTEGER PRIMARY KEY AUTOINCREMENT, link TEXT, first_seen DATE, last_seen DATE, address TEXT, address_ort TEXT, address_kommun TEXT, firma TEXT, maklare TEXT, utgangspris INTEGER, boarea INTEGER, biarea INTEGER, tomtarea INTEGER, rum INTEGER, driftskostnad INTEGER, alive TEXT")
		Database.db_create_table("datapoints", "id INTEGER PRIMARY KEY AUTOINCREMENT, object INTEGER, date DATE, hits INTEGER, FOREIGN KEY(object) REFERENCES objects(id)")
	end

	def Database.db_get_live_objects
		@db.execute( "SELECT link FROM objects WHERE alive='YES'" ).flatten
	end

	def Database.db_export_objects
		# Export all objects as CSV, let each row hold the information about one object.
		data = @db.execute( "SELECT * FROM objects" )
		data.each { |e| e[1] = "http://www.hemnet.se" + e[1] }
		data.map { |e| e.join(";") }.join("\n")
	end

	def Database.db_export_datapoints
		# For all objects, export a series of datapoints with date & time and hits
		# 1) Get list of objects to get
		# 2) Get data for object
		# 3) Export data

		data = @db.execute( "SELECT date, object, hits FROM datapoints")
		# objects = @db.execute( "SELECT id FROM objects" ).flatten
		# data = objects.map { |o| @db.execute( "SELECT object, date, time, hits FROM datapoints WHERE object='#{o}'") }
		# # Only the hour is of interest, filter out the rest
		# data.each { |e| e.each { |f| f[2] = f[2].truncate_minutes } }
		print data
		# data = @db.execute( "SELECT date, time, hits FROM datapoints WHERE object='#{primary_key}'" )
		# data.each { |e| p e }
	end

	def Database.db_close
		@db.close
	end

	# Add an object to database if not already there
	def Database.web_add_object(link)
		# Check if link aleady exist and exit if so
		result = @db.execute( "SELECT id FROM objects WHERE link='#{link}'" )
		return if (result.size != 0)

		page = get_html(link)

		pris = page.css('.property__price').text.gsub(/[[:space:]]/, '').to_i

		# Info about object
		# Address
		address = page.css('.property__address').text
		address_kommun = page.css('.property-location').css('.mr2').text
		# Clean up address, note: destroys the html node tree, therefore we clone it
		address_node = page.css('.property-location').dup
		address_node.at("a").replace("")
		address_node.at("span").replace("")
		address_ort = address_node.text.strip.split(',').at(0)

		# Create hashmap with info about object by zipping the information in the table
		info = Hash[ page.css('.property__attributes-container').xpath('.//dt').map { |e| e.text.strip.downcase }.slice(1..-2).zip(
			         page.css('.property__attributes-container').xpath('.//dd').map { |e| e.text.strip.clean_digits.to_i }.slice(1..-2) ) ]
		info.default = 0

		# Mäklarefirma
		firma = page.css('.broker').css('.broker-link').map { |e| e["href"] }.first
		maklare = page.css('.broker').xpath('.//p/b').text

	    @db.execute( "INSERT OR IGNORE INTO objects VALUES ( NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )", link, now(), now(), address, address_ort, address_kommun, firma, maklare, pris, info["boarea"], info["biarea"], info["tomtarea"], info["antal rum"], info["driftkostnad"], 1)
		Log.write "Added '#{link}' to table 'objects'"
	end

	# Retrieves a search page and enters the results found into the object table in the database
	def Database.web_update_objects(url)
		page = Nokogiri::HTML(HTTParty.get(url))
		links = page.css('ul#search-results').xpath('//a[@class="item-link-container" and not(@data-tally-path)]').map { |e| e["href"] }
		links.each { |link| Database.web_add_object(link) }
		# Mark all items as no longer alive (0) first and then mark the ones still in list as alive (1)
		@db.execute( "UPDATE objects SET alive = 'NO'" )
		links.each { |link| @db.execute( "UPDATE objects SET alive = 'YES' WHERE link='#{link}'" ) }
		links.each { |link| @db.execute( "UPDATE objects SET last_seen='#{now()}' WHERE link='#{link}'" ) }
	end

	def Database.web_add_datapoint(link)
		# Get data from object page
		page = get_html(link, true)
		object_id = @db.execute( "SELECT * FROM objects WHERE link='#{link}'" ).first.first
		hits = page.css('.property-stats__visits').text.gsub(/[[:space:]]/, '').to_i
		@db.execute("INSERT OR IGNORE INTO datapoints VALUES ( NULL, ?, ?, ? )", object_id, now(), hits )
	end
end

# MAIN
##########

options = { :url => "urls.txt" }

OptionParser.new do |opts|
  opts.banner = "Usage: dromhus.rb [options]"

  opts.on("-s", "--scrape", "Scrape web") do |s|
    options[:scrape] = s
  end

  opts.on("-u", "--url", "File containing the url(s) to scrape, default: 'urls.txt'") do |u|
    options[:url] = u
  end

  opts.on("-o", "--object", "Export object data as csv") do |o|
    options[:object] = o
  end

  opts.on("-d", "--datapoints", "Export datapoints data as csv") do |d|
    options[:datapoints] = d
  end

end.parse!

# File with urls must exist
if (File.exists?(options[:url]) == false)
	puts "url file '#{options[:url]}' missing"
	exit 1
end

Dir.mkdir 'data' unless File.exists?('data')

Log.new

if (options[:scrape] != nil)
	# make sure database tables exist 
	Database.db_create_tables
	# Add new and update still 'live' objects in database
	get_file(options[:url]).split("\n").each { |url| Database.web_update_objects(url) }
	# Add datapoints for all 'live' objects
	Database.db_get_live_objects.each { |link| Database.web_add_datapoint(link) }
end

if (options[:object] != nil)
	print Database.db_export_objects
end

if (options[:datapoints] != nil)
	print Database.db_export_datapoints
end
