from dask.distributed import Client
import time
import dask.array as da
import numpy as np

t0 = time.time()
client = Client('172.31.32.70:8786')

data = da.random.random_integers(99, 999, size=(5000, 5000), chunks=(1000, 1000))

future = client.compute(data)
result = np.array(future.result())

nilaicari = 100

for idx, i in enumerate(result):
    client.compute(result[idx].sort())


def BinarySearch(lys, val):
    listchunk = []
    for i, data in enumerate(lys):
        first = 0
        last = len(lys[i]) - 1
        index = -1
        while (first <= last) and (index == -1):
            mid = (first + last) // 2
            if lys[i][mid] == val:
                index = mid
            else:
                if val < lys[i][mid]:
                    last = mid - 1
                else:
                    first = mid + 1
        listchunk.append(index)
    return listchunk


big_future = client.scatter(result)
searchresult = client.submit(BinarySearch, big_future, nilaicari)
listindextemp = client.gather(searchresult)


def SearchIndex(data, cari, lc):
    listindex = []
    for idx, i in enumerate(data):
        tempidx = idx
        while data[idx][lc[tempidx]] == cari:
            text = "Index [" + idx.__str__() + "][" + lc[tempidx].__str__() + "]"
            listindex.append(text)
            lc[tempidx] = lc[tempidx] - 1
    return listindex


resultakhir = client.submit(SearchIndex, big_future, nilaicari, listindextemp)
finalresult = client.gather(resultakhir)

print(finalresult)

finish = time.time() - t0

for i in finalresult:
    print(i)

print(finish)
