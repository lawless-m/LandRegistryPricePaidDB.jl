module LandRegistryPricePaidDB

using CSV
using DataFrames
using Dates
using Pipe
using Serialization

LR = LandRegistryPricePaidDB
export LR

const LANDREGDIR = "/home/matt/wren/UkGeoData/landreg/"
const DATADIR = joinpath(LANDREGDIR, "complete")
const NEWESTDIR = joinpath(LANDREGDIR, "newest_per_address")
const TDATADIR = joinpath(LANDREGDIR, "test")
const mask = 0x00ffffffffffffff

export csv_files

#==
    see https://www.gov.uk/guidance/about-the-price-paid-data#explanations-of-column-headers-in-the-ppd
    TUID
        Transaction unique identifier 	
        A reference number which is generated automatically recording each published sale. 
        The number is unique and will change each time a sale is recorded.
    Property Type 	
        D = Detached, S = Semi-Detached, T = Terraced, F = Flats/Maisonettes, O = Other 
    old / new
        Y = a newly built property, N = an established residential building
    duration
        F = Freehold, L= Leasehold etc.
    paon
        Primary Addressable Object Name (e.g. house number "159")
    saon 
        Secondary Addressable Object Name (e.g. "Flat 7")
    ppd category
        A = Standard Price Paid
        B = Additional Price Paid
=##
const header = ["TUID", "price", "date", "postcode", "propertyType", "oldOrNew", "duration", "paon", "saon", "street", "locality", "townCity", "district", "country", "PPDcategory", "recordStatus"]

csv_files(datadir=DATADIR) = filter(f->endswith(f, ".csv"), readdir(datadir))

function read_csvs(datadir=DATADIR)
    files = csv_files(datadir)
    dfs = Vector{DataFrame}(undef, length(files))
    
    int(t) = ismissing(t) ? missing : parse(Int64, t)
    date(t) = ismissing(t) ? missing : Date(split(t, " ")[1], "yyyy-mm-dd")

    header_t = filter(h->!(h in ["price", "date"]), header)
    append!(header_t, ["price_t", "date_t"])

    Threads.@threads for i in 1:length(files)
        dfs[i] = @pipe CSV.read(files[i], DataFrame; header_t, types=String) |>
                    transform!(_, [
                        :date_t => ByRow(t->date(t)) => :date,
                        :price_t => ByRow(t->int(t)) => :price,
                        [:postcode, :paon, :saon, :street] => ByRow((a,b,c,d)->hash((a,b,c,d))) => :address_hash) 
                    ]) |>
                    select!(_, Not([:TUID, :date_t, :price_t]))
    end
    dfs
end

function consolidate(dfs)
    df = empty(DataFrame([
        :address_hash=>[zero(UInt64),zero(UInt64)],
        :date => [now(), now()], 
        :price => [0, missing], 
        :postcode => ["", missing],
        :propertyType => ["", missing],
        :oldOrNew => ["", missing],
        :duration => ["", missing],
        :paon =>  ["", missing],
        :saon  => ["", missing],
        :street => ["", missing],
        :locality => ["", missing],
        :townCity => ["", missing],
        :district => ["", missing],
        :country => ["", missing],
        :PPDcategory => ["", missing],
        :recordStatus => ["", missing]
        ]
        ))
    reduce((df, d)->append!(df, d), dfs, init=df)
end

function create_df(datadir=DATADIR, df_file="LandReg.df")
    @pipe read_csvs(datadir) |> consolidate(_) |> save(joinpath(datadir, df_file), _)
end

function save(io::IO, object)
    serialize(io, object)
    object
end

function save(memofile::AbstractString, object)
    try
        open(memofile, "w+") do io
           return save(io, object)
        end
    catch e
        println(stderr, e)
    end
    object
end

function load(memofile="")
    if memofile == ""
        memofile = joinpath(DATADIR, "LandReg.df")
    end
    if filesize(memofile) > 0
        open(memofile, "r") do io
            return deserialize(io)
        end
    end
end

#==
grep address_hash landreg_hashed.csv > landreg_sorted_uniq.csv
grep -v address_hash landreg_hashed.csv | 
    sort -t ',' -k 1,2 | 
    awk ' BEGIN { FS=","; previd=""; prev=""}  $1 != previd { print prev; prev=$0; previd=$1; next}  { prev=$0 } ' | grep -v "^$" >> landreg_sorted_uniq.csv
==#

#####
end
