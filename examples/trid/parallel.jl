include("serial.jl")
include("../../src/star.jl")

#solves Ax = b in parallel, where A is a symmetric tridiagonal matrix
#upper triangular solve 
function usolve(y, z, c, d, l, x)
  n = size(x, 1)
  #y, z are solutions at the previous and next separators
  x[n] = z
  @inbounds for i = n - 1 : -1 : 1
    x[i] = (x[i] - c[i + 1] * x[i + 1] - l[i] * y) / d[i] # 5 flops
  end
end

#lower triangular solve 
function lsolve(a, b, c, d, l, x)
  n = size(x,1)
  d[1] = a[1]
  x[1] = b[1]
  f = c[1]
  l[1] = f
  r = f / d[1]
  s = f * r
  y = x[1] * r
  @inbounds for i = 2 : n - 1
    l_ = c[i] / d[i - 1]         # 1
    d[i] = a[i] - c[i] * l_      # 2
    x[i] = b[i] - x[i - 1] * l_  # 2
    f *= -l_                     # 1 excluding negation for fillin
    l[i] = f                     # store fillin
    r = f / d[i]                 # 1 
    s += f * r                   # 2 
    y += x[i] * r                # 2	
  end
  l_ = c[n] / d[n - 1]
  d[n] = a[n] - c[n] * l_	
  x[n] = b[n] - x[n - 1] * l_
  f = - r * c[n]  # fillin between previous and next separators

  s, y, f, d[n], x[n]
end

function root(s, y, f, d, x)
  #solve the reduced tridiagonal system
  #d is the diagonal of the reduced system, f the subdiagonal, and
  #x the reduced right hand side 
  d[1:end - 1] -= s[2:end]
  x[1:end - 1] -= y[2:end]
  serial_trid(d, x, f[2:end])
  n = length(x)
  x = [0; x]
  return [(x[i], x[i+1]) for i = 1 : n]
end


trid_star(a, b, c, d, l, x) = Star.map(usolve, 
                    root(Star.reduce(lsolve, a, b, c, d, l, x)...), c, d, l, x)

#test routine
function trid_star_test(n)
  P = nworkers() # of processors
  #setup the inputs and outputs
  Da = DArray(I->100 * rand(map(length,I)), (n,), workers(), P, f1)
  Db = DArray(I->rand(map(length,I)), (n,), workers(), P, f1)
  Dc = DArray(I->rand(map(length,I)), (n + 1,), workers(), P, f2)
  Dd = DArray(I->zeros(map(length,I)), (n,), workers(), P, f1)
  Dl = DArray(I->zeros(map(length,I)), (n,), workers(), P, f1)
  Dx = DArray(I->zeros(map(length,I)), (n,), workers(), P, f1)

  t = @elapsed trid_star(Da, Db, Dc, Dd, Dl, Dx)

  # code to test the results
  x = convert(Array{Float64,1}, Dx)
  a = convert(Array{Float64,1}, Da)
  b = convert(Array{Float64,1}, Db)
  c = convert(Array{Float64,1}, Dc)
  d = convert(Array{Float64,1}, Dd)
  c = sub(c, 2:n)
  println("norm(A * x -b)", norm(matrix_vector_product(a, c, c, x) -b))
  println("serial trid - not storing L")
  x[:] = 0 ;
  t1 = @elapsed serial_trid(a, b, c, d, x)
  println("norm(A * x -b)", norm(matrix_vector_product(a, c, c, x) -b))
  println("serial time ", t1, " parallel time ", t)
  #
end

#auxiliary functions to partition the darrays among processors.
function f1(dims, procs) # auxiliary function to partition all darrays but the
                         # subdiagonal 
  n = dims[1]
  P = size(procs, 1)
  leaf_size::Int64 = max(floor(n / P), 2) #size of a leaf 
  cuts = ones(Int, P + 1) * leaf_size
  cuts[1] = 1
  cuts = cumsum(cuts)
  cuts[end] = n + 1
  indexes = Array(Any, P)
  for i = 1:P
    indexes[i] = (cuts[i] : cuts[i+1] - 1,)
  end
  indexes, Any[cuts]
end

function f2(dims, procs) # auxiliary function to partition the subdiagonal
  n = dims[1]
  P = size(procs, 1)
  leaf_size::Int64 = max(floor((n - 1) / P), 2) #size of a leaf 
  cuts = ones(Int, P + 1) * leaf_size
  cuts[1] = 1
  cuts = cumsum(cuts)
  cuts[end] = n + 1
  indexes = Array(Any, P)
  for i = 1:P
    indexes[i] = (cuts[i] : cuts[i+1] - 1,)
  end
  indexes, Any[cuts]
end

#overload the DArray constructor to provide user defined partitions.
function DArray(init, dims, procs, dist, distfunc::Function)
    np = prod(dist)
    procs = procs[1:np]
    idxs, cuts = distfunc([dims...], procs)

    chunks = Array(RemoteRef, dist...)
    for i = 1:np
        chunks[i] = remotecall(procs[i], init, idxs[i])
    end
    return DistributedArrays.construct_darray(dims, chunks, procs, idxs, cuts)
end
