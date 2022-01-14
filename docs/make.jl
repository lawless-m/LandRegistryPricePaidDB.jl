using Documenter
using LandRegistryPricePaidDB
using Dates


makedocs(
    modules = [LandRegistryPricePaidDB],
    sitename="LandRegistryPricePaidDB.jl", 
    authors = "Matt Lawless",
    format = Documenter.HTML(),
)

deploydocs(
    repo = "github.com/lawless-m/LandRegistryPricePaidDB.jl.git", 
    devbranch = "main",
    push_preview = true,
)
