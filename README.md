# Star.jl
Star is a programming model and an associated implementation to generate 
parallel code for programs like ``all-prefix-sums'' that operate on ordered sets
of data.
Star abstracts the pattern of computation and interprocessor communication 
in such programs, hides low level programming details, and offers ease of
expression, thereby improving programmer productivity in writing such
programs.

In addition to the numerous applications of all-prefix-sums, many real world 
programs are expressible in the Star model.
This work presents two such programs namely solving symmetric,
diagonally-dominant tridiagonal systems, and performing ``watershed cuts'' on 
graphs.
Users specify application-specific functions and their corresponding arguments 
as inputs to Star, which then composes them to automatically generate parallel 
code.
We implemented Star in the Julia programming language, which supports both
shared-memory and distributed-memory computing models alike.
Empirical results show that Star does not degrade performance or scalability
at the cost of ease of expression.

Unlike the MapReduce programming model that operates on unordered sets of data, 
Star operates under a restricted model where the global order of data is known 
apriori.
Consequently, MapReduce incurs asymptotically more overheads than Star for 
programs like all-prefix-sums.
To compare the performance of MapReduce with Star, we simulated MapReduce in Julia to compute the
all-prefix-sums operation. Empirical results on a modern Intel(R) Xeon(R) CPU E5-2676 v3 @ 2.40GHz machine
with 40 cores, 32KB L1 cache per core, 256 KB L2 cache per core, and 30MB L3 cache per socket,
show that the MapReduce simulator runs 3-4 times slower than its Star counterpart in computing all-prefix-sums
on an array of 10 billion elements.
(We gave the MapReduce simulator some advantage in that though the actual map reduce model would require sorting the elements to compute all-prefix-sums, the simulator doesn't sort. Otherwise, the simulator's performance would be even slower.)
Theoretically, given **p** processors to execute all-prefix-sums on
an array of **n** elements, Star incurs **Θ(n/p + p)** time and 
**Θ(p)** communication.
In contrast, MapReduce incurs **Θ(n/p lg n/p + p lg p)** 
time and **Θ(p^2)** communication.
When the number of processors **p=√n**, which gives the fastest runtime for
MapReduce, its communication grows to **Θ(n)**, which is 
just the serial time to compute all-prefix-sums.


#Example

As an example, consider the serial Julia cumsum function cumsum!(x,x), which computes the cumulative sum of the elements in the array x.
A parallel version of cumsum function (implemented using the star abstraction in examples/cumsum.jl) can be explained as follows.

Suppose we have 3 processors p1, p2, and p3, and that a global array x = [1,2,3,4,5,6] is distributed among them.
That is, p1 has [1,2], p2 has [3,4], and p3 has [5,6].
We are interested in finding the cumulative sum of the global array x.

The parallel cumsum algorithm has 3 steps.

1. In the first step, we compute the sum of the local array at each processor in parallel.
After this reduction, we collect the sums 3,7, and 11, from processors p1, p2, and p3 respectively, into an array S = [3,7,11].

2. In the second step, we compute an ``exclusive'' cumsum of the array S of 
sums, which results in a new array C = [0, 3, 10].

3. In the third step, we compute the cumsum of the local array at each processor "pi", using the element C[i] as an initial value.
For example, at p2, we compute the cumsum of the local array [3, 4] using C[2] = 3 as the initial sum, which results in [6, 10] as the answer. After the third step, we have computed the cumulative sums [1,3], [6,10], and [15,21] at p1, p2, and p3 respectively. 

Steps 1 and 3 are performed in parallel on the processors, whereas step 2 happens serially.

The parallel cumsum algorithm described above, can be expressed using the Star abstraction as the following one-line code.

Star.scan(cumsum, exclusive\_cumsum(Star.reduce(sum, x)), x)

where the cumsum and exclusive\_cumsum functions are defined by the user,
as shown in examples/cumsum.jl.

Other examples of the Star abstraction (in the examples directory) include 

1. utvw.jl, which computes x=u'vw on column vectors u, v, w, and x.

2. trid/parallel.jl which solves Ax=b where A is a symmetric, diagonally-dominant, tridiagonal matrix, and b and x are the rhs and solution respectively.

3. Watershed cuts -- To be added.








