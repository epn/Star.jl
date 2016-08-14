#solves Ax = b where A is a symmetric tridiagonal matrix
#a is the diagonal of A, c is the subdiagonal of A, b is the rhs, 
#d is the diagonal of U, x is the solution
function serial_trid(a, b, c, d, x)  
  n = size(a,1)
  d[1] = a[1]
  x[1] = b[1]
  for i = 2:n   # 3 reads and 2 writes
    l = c[i - 1] / d[i - 1] 
    d[i] = a[i] - c[i - 1] * l
    x[i] = b[i] - l * x[i - 1]
  end
          
  x[n] = x[n] / d[n]
  for i = n - 1 : -1 : 1    # 3 reads and 1 write 
    x[i] = (x[i] - c[i] * x[i + 1]) / d[i]
  end
end

#solves Ax = b in place, where A is a symmetric tridiagonal matrix
#a is the diagonal of A, overwritten with the diagonal of U
#c is the subdiagonal of A
#b is the rhs, overwritten with the solution
function serial_trid(a, b, c)  
  n = size(a,1)
  for i = 2:n
    l = c[i - 1] / a[i - 1]
    a[i] -= c[i - 1] * l
    b[i] -= l * b[i - 1]
  end

  b[n] /= a[n]
  for i = n - 1 : -1 : 1
    b[i] = (b[i] - c[i] * b[i + 1]) / a[i]
  end
end

#compute A*x, where A is a tridiagonal matrix
#a is the diagonal of A
#b is the super diagonal of A
#c is the sub diagonal of A
#x is the column vector
function matrix_vector_product(a, b, c, x)
  n = size(x, 1)
  y = zeros(n)
  for i = 1 : n
    y[i] = a[i] * x[i]
  end
  for i = 2 : n
    y[i] += c[i - 1] * x[i - 1]
  end
  for i = 1 : n - 1
    y[i] += b[i] * x[i + 1]
  end
  y
end
