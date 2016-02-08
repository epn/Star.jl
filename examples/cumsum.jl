include("../src/star.jl")
using DistributedArrays

g(a, z) = begin z[1] += a ; cumsum!(z,z) ; end 
csum(y) = [0; cumsum(y)]
# computes cumsum on a distributed array x
cumsum_star(x) = Star.map(g, csum(Star.reduce(sum, x)), x)

function cumsum_star_test(x)
  x_ = convert(Array{Float64, 1}, x)
  t = @elapsed cumsum_star(x)
  out_ = convert(Array{Float64, 1}, x)
  t1 = @elapsed cumsum!(x_, x_)
  println("parallel time ", t, " serial time ", t1)
  println("rel norm ", norm(out_-x_)/abs(x_[end]))
end

function cumsum_no_star(x) 
  ref = [@spawnat p sum(localpart(x)) for p in workers()] #sum local data
  y = [fetch(r) for r in ref] 
  cumsum!(y, y) #cumsum at the root
  y = [0; y]
  @sync [@spawnat workers()[i] g(y[i], localpart(x)) for i = 1 : nworkers()]
end

