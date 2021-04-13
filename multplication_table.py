from mpi4py import MPI
import time
import math
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

#outbuf = "Hello,word! from process% d of %d  " %(rank,size)

final_set = set()

starting_point=[]

scatter_slice=[]

rr_slice=[] # Contains remaining rows from unequal division of rows to processors

n = 50000  # Input size

print("Input size ", n)

jump= int(n/size) #jump to access starting points
#print("jump %s",jump)
if rank == 0:

    # Gather the starting points here for each of the processor

    for i in range (1,n+1,jump):
        starting_point.append(i)

    print("Starting point array comes out to be## ",starting_point)
    scatter_slice = starting_point[0:size]
    print("Scatter slice is -->",scatter_slice)

    if (n%size !=0):
        rr_slice = [x for x in range(starting_point[size],n+1)]
        print("The left out part is >> ",rr_slice)



#if(len(starting_point)>size):
 #   starting_point.pop()
data = comm.scatter(scatter_slice, root=0)



local_set=set()
local_arr=[]
local_arr1=[]
local_arr2=[]

#if (rank !=size-1):
for local_row in range(data,data+jump):
    for counter in range(local_row*local_row, (local_row*n)+1, local_row):
        local_set.add(counter)


        #else:
 #  for local_row in range(data,n+1):
  #      for counter in range(local_row, (local_row*n)+1,local_row):



  #         local_arr.append(counter)

#print("I am processor %s and I am handling arr %s" %(rank, local_arr))




if (rank == 0):
    if(n%size !=0):
        rr_slice= rr_slice
elif (n%size !=0):
    rr_slice = None

rr_slice = comm.bcast(rr_slice, root=0)

if (rank == 0):
    print("Broadcasting RR array")

elif (rank != 0):
#    print("RANK trying access", rank)
    if( n%size!=0):
        if (rank <=len(rr_slice)):
            for local_row in range(rr_slice[rank-1]*rr_slice[rank-1],(rr_slice[rank-1]*n)+1, rr_slice[rank-1]):
                local_set.add(local_row)


local_arr=list(local_set)
local_arr1=local_arr[0:len(local_arr)//2]
local_arr2=local_arr[len(local_arr)//2:]




newData1 = comm.gather(local_arr1, root=0)
newData2= comm.gather(local_arr2,root=0)

if rank ==0:
    for set_counter1 in newData1:
        final_set.update(set_counter1)
   # answer=len(final_set)
    #print("The answer for the problem is %s :) "% answer)
    for set_counter2 in newData2:
        final_set.update(set_counter2)

    answer = len(final_set)
    print("The answer is %s :)"% answer)

