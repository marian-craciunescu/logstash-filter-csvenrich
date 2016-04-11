# encoding: utf-8
require 'spec_helper'
require "logstash/filters/csvenrich"

describe LogStash::Filters::CSVEnrich do

    describe "load file that doesn't exist" do 
        let(:config) do <<-CONFIG
          filter {
              csvenrich {
                  "file" => "spec/fixtures/nofile.csv"
                  "key_col" => "value"
                  "lookup_col" => "id"
                  "map_field" => { "username" => "username" }
              }          
          }
          CONFIG
        end

        sample("id" => 1) do
          error = "No such file or directory - spec/fixtures/nofile.csv"
          expect { subject.register }.to raise_error("#{error}")
        end
     
    end

    describe "load empty file" do

         let(:config) do <<-CONFIG
           filter {
               csvenrich {
                   "file" => "spec/fixtures/empty.csv"
                   "key_col" => "value"
                   "lookup_col" => "id"
                   "map_field" => { "username" => "username" }
               }          
          }
          CONFIG
         end
        sample("id" => 1) do 
          expect { subject.register }.to raise_error("file is empty")
        end
    end

    describe "load file with header only" do

         let(:config) do <<-CONFIG
           filter {
               csvenrich {
                   "file" => "spec/fixtures/header_only.csv"
                   "key_col" => "value"
                   "lookup_col" => "id"
                   "map_field" => { "username" => "username" }
               }
          }
          CONFIG
         end
        sample("id" => 1) do
          expect { subject }.to_not raise_error
        end
    end

    describe "load file with 1 column" do

         let(:config) do <<-CONFIG
           filter {
               csvenrich {
                   "file" => "spec/fixtures/onecolumn.csv"
                   "key_col" => "value"
                   "lookup_col" => "id"
                   "add_field" => { "tag" => "possible_bad_actor" }
               }
          }
          CONFIG
         end
        sample("id" => "1") do
          expect { subject }.to_not raise_error
          expect(subject).to include("tag")
          expect(subject["tag"]).to eq("possible_bad_actor")
        end
    end

    describe "load file with 2 column" do

         let(:config) do <<-CONFIG
           filter {
               csvenrich {
                   "file" => "spec/fixtures/twocolumns.csv"
                   "key_col" => "value"
                   "lookup_col" => "id"
                   "map_field" => {"username" => "username"}
               }
          }
          CONFIG
         end
        sample("id" => "2") do
          expect { subject }.to_not raise_error
          expect(subject).to include("username")
          expect(subject["username"]).to eq("test2")
        end
    end

    describe "load file with columns" do

         let(:config) do <<-CONFIG
           filter {
               csvenrich {
                   "file" => "spec/fixtures/threecolumn.csv"
                   "key_col" => "value"
                   "lookup_col" => "id"
                   "map_field" => { "user" => "username" "firstname" => "[user][firstname]" "lastname" => "[user][lastname]" }
               }
          }
          CONFIG
         end
        sample("id" => "1") do
          expect { subject }.to_not raise_error
          expect(subject).to include("[user][firstname]")
          expect(subject["[user][firstname]"]).to eq("John")
          expect(subject["[user][lastname]"]).to eq("Doe")
        end
    end

 
end
