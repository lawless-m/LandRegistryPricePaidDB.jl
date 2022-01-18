using LandRegistryPricePaidDB
using Test

function buff()
    io = IOBuffer("""postcode,year,price
    AL10 0AB,2000,63000
    AL10 0AB,2003,126500
    AL10 0AB,2003,167000
    AL10 0AB,2003,177000
    AL10 0AB,2004,125000
    AL10 0AB,2013,220000
    AL10 0AB,2014,180000
    YO8 9YB,2021,269950
    YO8 9YD,2011,230000
    YO8 9YD,2012,249999
    YO8 9YD,2018,327500
    YO8 9YE,2009,320000
    YO8 9YE,2019,380000
    """)
    seekstart(io)
    io 
end

@testset "LandRegistryPricePaidDB.jl" begin
    kvs = create_kvs(buff())
    @test kvs[postcode_to_UInt64("YO8 9YB")].data ==  0x00000000000000a6
    @test kvs[postcode_to_UInt64("YO8 9YB")].aux ==  0x0000000000000001
    @test kvs[postcode_to_UInt64("AL10 0AB")].data ==  0x0000000000000014
    @test kvs[postcode_to_UInt64("AL10 0AB")].aux ==  0x0000000000000007
end
