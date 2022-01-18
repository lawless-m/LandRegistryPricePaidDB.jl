module LandRegistryPricePaidDB

using CSV
using DataFrames
using Dates
using Pipe
using Serialization
using Index1024

LR = LandRegistryPricePaidDB
export LR

const LANDREGDIR = "/home/matt/wren/UkGeoData/landreg/"
const DATADIR = joinpath(LANDREGDIR, "complete")
const NEWESTDIR = joinpath(LANDREGDIR, "newest_per_address")
const TDATADIR = joinpath(LANDREGDIR, "test")
const mask = 0x00ffffffffffffff

export csv_files, postcode_to_UInt64, create_kvs

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
    
    int(t) =  ismissing(t) ? missing : parse(Int64, t)
    date(t) = ismissing(t) ? missing : Date(split(t, " ")[1], "yyyy-mm-dd")

    Threads.@threads for i in 1:length(files)
        dfs[i] = @pipe CSV.read(files[i], DataFrame; header_t, types=String) |>
                    transform!(_, [
                        :date => ByRow(t->date(t)) => :date_t,
                        :price => ByRow(t->int(t)) => :price_t,
                        [:postcode, :paon, :saon, :street] => ByRow((a,b,c,d)->hash((a,b,c,d))) => :address_hash
                    ]) |>
                    select!(_, Not([:TUID, :date, :price])) |>
                    rename!(_, [:price_t=>:price, :date_t=>:date])
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
    
some have no postscodes

awk ' BEGIN { FS="," } $4 != "" { gsub(/-.*/, "", $2); print $4","$2","$3 }' landreg_sorted_uniq.csv | sort > pc_year_price.csv
==#


function postcode_to_UInt64(pc) 
    if ismissing(pc)
        return 0
    end
    m = match(r"([A-Z]+)([0-9]+([A-Z]+)?) ?([0-9]+)([A-Z]+)", replace(pc, " "=>""))
    if m == nothing || m[1] === nothing || m[2] === nothing || m[4] === nothing || m[5] === nothing
        return 0
    end
    reduce((a,c) -> UInt64(a) << 8 + UInt8(c), collect(lpad(m[1], 2) * lpad(m[2], 2) * m[4] * m[5]), init=0)
end

function UInt64_to_postcode(u)
    if u == 0
        return ""
    end
    cs = Char[]
    while u > 0
        push!(cs, Char(u & 0xff))
        u >>= 8
    end
    part(cs) = replace(String(reverse(cs)), " "=>"")
    "$(part(cs[4:end])) $(part(cs[1:3]))"
end

function count_lines(io, pcode)
    lines = 0
    pos = position(io)
    while (line = readline(io)) !== nothing
        lines += 1
        newcode = split(line, ",", limit=2)[1]
        if newcode != pcode
            return newcode, lines, pos
        end
        pos = position(io)
    end
    return "", lines, pos
end

#==
LR.build_index_file(joinpath(LR.LANDREGDIR, "Postcode_Year_Price.index"), LR.create_kvs(joinpath(LR.LANDREGDIR, "landreg_pc_year_price.csv")))
==#

create_kvs(fname) = open(create_kvs, fname)

function create_kvs(io::IO)
    kvs = Dict{UInt64, DataAux}()
    readline(io)
    pos = position(io)
    pcode, lines, nextpos = count_lines(io, "postcode")
    while pcode != ""
        newcode, lines, nextpos = count_lines(io, pcode)
        kvs[postcode_to_UInt64(pcode)] = (data=pos, aux=lines)
        pcode = newcode
        pos = nextpos
    end
    kvs
end

function csv(io::IO, offset)
    buff = IOBuffer()
    write(buff, readline(io; keep=true))    
    seek(io, offset)
    write(buff, readline(io; keep=true))
    seekstart(buff)
    CSV.File(buff)
end

prices_for_postcode(idx, pcode, csvfile) = open(csvfile) do io prices_for_postcode(idx, pcode, io) end
        
function prices_for_postcode(idx, pcode, csvio::IO)
    (offset, lines) = get(idx, postcode_to_UInt64(pcode), (0,0))
    if lines > 0
        seek(csvio, offset)
        return CSV.File(csvio; header=["postcode", "year", "price"], limit=lines)
    end
end

#####
end
