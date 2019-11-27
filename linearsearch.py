from dask.distributed import Client
import time
import dask.array as da

t0 = time.time()
client = Client('172.31.32.70:8786')

data = da.random.random_integers(99, 999, size=(20000, 20000), chunks=(1000, 1000))

future = client.compute(data)
result = future.result()

nilaicari = 100


def search(hasil, cari):
    listdata = []

    for idx, i in enumerate(hasil):
        for idx2, x in enumerate(i):
            if x == cari:
                text = "Index [" + idx.__str__() + "][" + idx2.__str__() +"]"
                listdata.append(text)

    return listdata


big_future = client.scatter(result)
searchresult = client.submit(search, big_future, nilaicari)
finalresult = client.gather(searchresult)

finish = time.time() - t0

for i in finalresult:
    print(i)
print(finish)
