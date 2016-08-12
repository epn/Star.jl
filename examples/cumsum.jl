include("../src/star.jl")
using DistributedArrays

function exclusive_cumsum(x)
  sum = 0
  for i = 1 : length(x)
    temp = x[i]
    x[i] = sum
    sum = sum + temp
  end
  x
end

function cumsum(initial_value, x)
  if length(x) > 0
    x[1] = initial_value + x[1]
  end
  cumsum!(x, x)
end

# computes cumsum on a distributed array x
cumsum_star(x) = Star.scan(cumsum, exclusive_cumsum(Star.reduce(sum, x)), x)

function cumsum_star_test(x)
  x_ = convert(Array{Float64, 1}, x)
  t = @elapsed cumsum_star(x)
  out_ = convert(Array{Float64, 1}, x)
  t1 = @elapsed cumsum!(x_, x_)
  println("parallel time ", t, " serial time ", t1)
  println("rel norm ", norm(out_-x_)/abs(x_[end]))
end

function cumsum_no_star(x) 
  ref = Array(Any, nworkers())
  for i = 1 : nworkers()
    ref[i] = @spawnat workers()[i] sum(localpart(x))
  end
  y = [fetch(r) for r in ref] 
  exclusive_cumsum(y)
  @sync begin 
    for i = 1 : nworkers()
      @spawnat workers()[i] cumsum(y[i], localpart(x))
    end
  end
end
