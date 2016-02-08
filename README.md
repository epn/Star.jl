# Star.jl
Star is an abstraction in Julia for expressing parallel programs that are parallel-prefix like in structure and computation. Star hides the complexity of Julia's parallel programming model, and presents an easy-to-use abstraction to express parallel programs. This is joint work with Andreas Noack and Alan Edelman.

As an example, consider the serial Julia cumsum function cumsum!(x,x), which computes the cumulative sum of the elements in the array x.
A parallel version of cumsum function (implemented using the star abstraction in examples/cumsum.jl) can be explained as follows.

Suppose we have 3 processors p1, p2, and p3, and that a global array x = [1,2,3,4,5,6] is distributed among them.
That is, p1 has [1,2], p2 has [3,4], and p3 has [5,6].
We are interested in finding the cumulative sum of the global array x.

The parallel cumsum algorithm has 3 steps.

1. In the first step, we compute the sum of the local array at each processor in parallel.
After this reduction, we collect the sums 3,7, and 11, from processors p1, p2, and p3 respectively, into an array S = [3,7,11].

2. In the second step, we compute a cumsum of the array S of sums, which results in a new array C = [3, 10, 21].
Besides, we augment the array C with a dummy value 0 at the front, which results in C = [0, 3, 10, 21].

3. In the third step, we compute the cumsum of the local array at each processor pi, using the element C[i] as an initial sum.
For example, at p2, we compute the cumsum of the local array [3, 4] using C[2] = 3 as the initial sum, which results in [6, 10] as the answer. After the third step, we have computed the cumulative sums [1,3], [6,10], and [15,21] at p1, p2, and p3 respectively. 

Steps 1 and 3 are performed in parallel on the processors, whereas step 2 happens serially.

The parallel cumsum algorithm described above, can be expressed using the Star abstraction as the following one-line code.

Star.map(g, f(Star.reduce(sum, x)), x)

where, we have the following definitons for functions f and g.

f(y) = [0; cumsum(y)]

g(a, z) = begin z[1] += a ; cumsum!(z,z) ; end 

Note that the user doesn't have to worry about using "spawns", "fetches", and "syncs", when programming with this abstraction.

Other examples in the examples directory include 

1. utvw.jl, which computes x=u'vw on column vectors u, v, w, and x.

2. trid/parallel.jl which solves Ax=b where A is a symmetric, diagonally-dominant, tridiagonal matrix, and b and x are the rhs and solution
respectively.








