using DistributedArrays

module Star

import DistributedArrays:localpart
localpart(x) = x
#=
scan takes as input a function f that should be applied on the processors, 
the data 'data' that should be distributed among the processors and passed as 
an argument to f, 
and the rest of the arguments 'args' for f.
=#
function scan(f, data, args...)
  assert(is_tuple(args))
  @sync begin 
    for i = 1 : nworkers()
      in = length(data) >= nworkers() ? data[i] : data
      @spawnat workers()[i] f(in..., Base.map(localpart, args)...)
    end
  end
end

#=
reduce takes as input a function f that should be applied on the processors,
and the arguments 'args' for f.
It collects the result of applying the function f at each processor into
an array(or arrays), and returns a tuple of the array(or arrays). 
=#
function reduce(f, args...) 
  assert(is_tuple(args))
  rref = [@spawnat p f(Base.map(localpart, args)...) for p in workers()]
  root_in = [fetch(r) for r in rref]
  sz = size(root_in[1], 1)
  #convert array of tuples into a 2D array
  in = Array(Any, nworkers(), sz)
  for i = 1 : nworkers()
    for j = 1 : sz
      in[i,j] = root_in[i][j]
    end
  end
  #dynamically generate the function arguments from the 2D array
  ex = "$(in[:, 1])"
  for j = 2 : sz
    ex = ex * ", $(in[:, j])"
  end
  return eval(parse(ex))
end

function is_tuple(x::Tuple)
  true
end

function is_tuple(x)
  false
end

end

