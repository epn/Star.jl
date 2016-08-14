using DistributedArrays

#Code for cumsum using mapreduce.
#We assume that a global array A is stored in a sequence of P partitions 
#A1, A2, ..., Ap, where each partition is local to a processor.
#The elements in each subarray Aj are stored in order.

#a map function for the 1st step
#key is a (i,j) pair, and the value Aij is the ith element in the jth partition 
map(key::Tuple{Int64,Int64}, Aij::Float64) = (key[2], (key[1], Aij)) 

#a combine function to compute the sum of the elements in a partition.
#sum takes as input a key, which is the partition id, and an array of values.
#It returns a tuple (1, (key, sum_of_values)), where the artifical key 1
#is used to guarantee that the same reduce worker receives the tuples
#from all the processors.
sum(key::Int64, data) = (1, (key, Base.sum(data))) #sum the values

#a reduce function to compute the exclusive cumsum of the partial sums.
#It takes as input a key, and an array of (partition_id, value) tuples,
#where the partition ids are not necessarily in sorted order.
#It sorts the array by the partition id, and computes the cumsum
#of the values in the sorted array
function exclusive_cumsum(key, value)
  #sort value by partition id
  sort!(value, lt=(a,b)->a[1]<b[1])
  #compute the exclusive cumsum
  initial_value = 0
  for i = 1:length(value)
    temp = value[i][2]
    value[i] = (value[i][1], initial_value)
    initial_value = initial_value + temp
  end
end

#invoke the map function f and combine function g
#map_and_combine(id, f, g, A) = g(id, [f((i, id), A[i])[2][2] for i = 1 : length(A)]) 
map_and_combine(id, f, g, A) = sum(id, [map((i, id), A[i])[2][2] for i = 1 : length(A)]) 

#a combine function for the 2nd step
#key is the partition id, data is the array of values 
#initial_value is the sum of elements in partitions 1...j-1
function cumsum(key, data, initial_value) 
  if length(data) > 0
   data[1] += initial_value
  end
  cumsum!(data, data)
end

function map_and_combine(id, f, g, A, R)
  #A[:] = [f((i, id), A[i])[2][2] for i = 1 : length(A)]
  #[A[i] = map((i, id), A[i])[2][2] for i = 1 : length(A)]
  A[:] = [map((i, id), A[i])[2][2] for i = 1 : length(A)]
  initial_value = R[id][2]
  #g(id, A, initial_value)
  cumsum(id, A, initial_value)
end

function test(A)
#  A_ = convert(Array{Float64, 1}, A)
  t = @elapsed begin
    #mapreduce step 1
    ref = [@spawnat workers()[i] map_and_combine(i, map, sum, localpart(A)) for i =  1:nworkers()]
    #in the reduce step, compute the exclusive cumsum 
    partial_sum = [fetch(ref[i])[2] for i = nworkers():-1:1]
    exclusive_cumsum(1, partial_sum) 
    #mapreduce step 2
    @sync begin
      [@spawnat workers()[i] map_and_combine(i, map, cumsum, localpart(A), partial_sum) for i =  1:nworkers()]
    end
  end
  println("parallel time ", t) ;
#  out_ = convert(Array{Float64, 1}, A)
#  t1 = @elapsed cumsum!(A_, A_)
#  println("parallel time ", t, " serial time ", t1)
#  println("rel norm ", norm(out_-A_)/abs(A_[end]))
end
