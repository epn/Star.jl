using DistributedArrays

#Code for cumsum using mapreduce.
#We assume that a global array A is stored in a sequence of P partitions 
#A1, A2, ..., Ap, where each partition is local to a processor.
#The elements in each subarray Aj are stored in order.

#a map function for the 1st step
#value Aij is the ith element in the jth partition 
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
  value
end

function map_and_combine(f, g, A)
  #B = [f(i, A[i])[2] for i = 1 : length(A)]
  #g(myid(), B) #invoke the combine function
  #invoke the map and combine functions
  id = myid()
  result = g(id, [f((i, id), A[i])[2][2] for i = 1 : length(A)]) 
  @show result
  result
end

#A map function cumsum.
#cumsum computes the cumsum of the augmented array [d0 data], where
#d0 is the sum of the elements to the left of data
function cumsum(d0, data)
  data[1] += d0
  cumsum!(data, data)
end

function test(n)
  A=dones(n)
  ref = Array{RemoteRef}(nworkers()) 
  #mapreduce step 1
  for i = 1:nworkers()
    p = workers()[i]
    ref[i] = @spawnat p map_and_combine(map, sum, localpart(A))
  end
  #In the reduce step, compute the exclusive cumsum 
#=
  temp = [fetch(ref[i]) for i = nworkers():-1:1]
  partial_sum = Array{Tuple{Int64, Float64}}(nworkers())
  for i = nworkers():-1:1
    @show temp[i]
    partial_sum[i] = temp[i][2] 
    @show partial_sum[i]
  end
=#
  partial_sum = [fetch(ref[i])[2] for i = nworkers():-1:1]
  @show partial_sum
  result = exclusive_cumsum(1, partial_sum) 
  @show result 
  #=
  #map again
  @sync begin
    for i = 1:nworkers()
      p = workers()[i]
      assert(partial_sum[i][1] == i - 1)
      @spawnat p cumsum(partial_sum[i][2], localpart(A))
    end
  end
  A_ = convert(Array{Float64, 1}, A)
  @show A_
  =#
end
