include("../src/star.jl")
using DistributedArrays

scalar_times_vector(a, x, w) = x[:] = a * w

#compute x=u'vw
utvw_star(u, v, w, x) = Star.map(scalar_times_vector, sum(Star.reduce(dot, u, v)), x, w) 

function utvw_star_test(u, v, w, x)
  #compute x=u'vw
  t = @elapsed utvw_star(u, v, w, x)

  #code to test the results
  a = convert(Array{Float64, 1}, u)
  b = convert(Array{Float64, 1}, v)
  c = convert(Array{Float64, 1}, w)
  d = convert(Array{Float64, 1}, x)

  e = zeros(Float64, length(a))
  t1 = @elapsed begin 
    e = dot(a, b) * c
  end

  println("parallel time ", t, " serial time ", t1)
  println("rel norm ", norm(d - e)/norm(e))
end

function utvw_no_star(u, v, w, x)
  remote_ref = [@spawnat p dot(localpart(u), localpart(v)) for p in workers()]
  partial_dot_product = [fetch(r) for r in remote_ref]
  s = sum(partial_dot_product)
  @sync [@spawnat p scalar_times_vector(s, localpart(x), localpart(w)) for p in workers()]
end

