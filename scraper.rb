#!/bin/env ruby
# encoding: utf-8

require 'scraperwiki'
require 'nokogiri'
require 'open-uri'
require 'colorize'
require 'rest-client'

require 'pry'
require 'open-uri/cached'
OpenURI::Cache.cache_path = '.cache'

@API_URL = 'http://api.parldata.eu/kv/kuvendi/%s'

def noko_q(endpoint, h)
  result = RestClient.get (@API_URL % endpoint), params: h
  doc = Nokogiri::XML(result)
  doc.remove_namespaces!
  entries = doc.xpath('resource/resource')
  return entries if (np = doc.xpath('.//link[@rel="next"]/@href')).empty?
  return [entries, noko_q(endpoint, h.merge(page: np.text[/page=(\d+)/, 1]))].flatten
end

def overlap(mem, term)
  mS = mem[:start_date].to_s.empty?  ? '1000-01-01' : mem[:start_date]
  mE = mem[:end_date].to_s.empty?    ? '2999-01-01' : mem[:end_date]
  tS = term[:start_date].to_s.empty? ? '1000-01-01' : term[:start_date]
  tE = term[:end_date].to_s.empty?   ? '2999-01-01' : term[:end_date]

  return unless mS <= tE && mE >= tS
  [mS, mE, tS, tE].sort[1,2]
end

# http://api.parldata.eu/kv/kuvendi/organizations?where={"classification":"chamber"}
xml = noko_q('organizations', where: %Q[{"classification":"chamber"}] )
xml.each do |chamber|
  term = { 
    id: chamber.xpath('.//id').text,
    name: chamber.xpath('.//name').text.sub('Kuvendit të Kosovës - ',''),
    start_date: chamber.xpath('.//founding_date').text,
    end_date: chamber.xpath('.//dissolution_date').text,
    source: chamber.xpath('.//sources/url').text,
  }
  ScraperWiki.save_sqlite([:id], term, 'terms')

  # http://api.parldata.eu/kv/kuvendi/memberships?where={"organization_id":"chamber_2001-11-17"}&embed=["person.memberships.organization"]
  mems = noko_q('memberships', { 
    where: %Q[{"organization_id":"#{term[:id]}"}],
    max_results: 50,
    embed: '["person.memberships.organization"]'
  })

  mems.each do |mem|
    person = mem.xpath('person')
    data = { 
      id: person.xpath('id').text,
      birth_date: person.xpath('birth_date').text,
      name: person.xpath('name').text,
      sort_name: person.xpath('sort_name').text,
      family_name: person.xpath('family_name').text,
      given_name: person.xpath('given_name').text,
      image: person.xpath('image').text,
      image: person.xpath('image').text,
      source: person.xpath('sources/url').first.text,
    }

    person.xpath('memberships[organization[classification[text()="parliamentary_group"]]]').each do |pmem|
      mem = { 
        term: term[:id],
        start_date: pmem.xpath('start_date').text,
        end_date: pmem.xpath('end_date').text,
        party: pmem.xpath('organization/name').text,
        party_id: pmem.xpath('organization/id').text,
      }
      next unless range = overlap(mem, term)
      row = data.merge(mem)
      ScraperWiki.save_sqlite([:id, :term, :start_date], row)
    end
  end

end

