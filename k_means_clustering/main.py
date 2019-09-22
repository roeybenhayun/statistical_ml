import scipy.io
import matplotlib.pyplot as plt
import numpy as np
import os
import matplotlib.pyplot as plt
import math
from tempfile import TemporaryFile

class KMeansClustering:
    def __init__(self, distribution = None, data_dir_path=None ):
        self.__data_dir_path = data_dir_path 
        self.__data_file_list = ["allSamples.mat"]
        self.__print_dims = False
        self.__plot = False
        self.__save = False
        self.__unlabeled_data=[]
        self.__distances = []
        self.__clusters = []
        self.__cluster_init_strategy = 1
        self.__k = 2

        if (data_dir_path is None):
            # assume data dir is in the current directory
            self.__data_path = os.path.dirname(os.path.realpath(__file__))
            # make sure required file are in the data_dir_path
            for mat_file in self.__data_file_list:
                if (False == os.path.isfile(mat_file)):
                    print ("Missing file : " + str(mat_file))


    def setup(self,cluster_init_strategy, k):
        """
        Initial setup for the KMeans Clustering algorithm
        Parameters
        ----------
        cluster_init_strategy, the selection used for the initial cluster center
                1 - randomally pick the initial canters from the givan examples
                2 - pick the first randomally
        k, number of clusters (2-10)        
        """
        if (cluster_init_strategy != 1 and cluster_init_strategy !=2):
            print ("Invalid cluster selection strategy. Please supported method")
            return
        if ( k<2 or k>10):
            print ("Invalid number of clusters selection. Supported range is 2-10.")
            return
        
        self.__k = k
        self.cluster_init_strategy = cluster_init_strategy

        self.load_data()

        # init distance 2d Nxk array (where N is the number of samples and k is the number of clusters)
        self.__distances = np.zeros([self.__unlabeled_data.shape[0],k])
        print("self.__distances", self.__distances.shape)

        self.__min_distances = np.zeros([self.__unlabeled_data.shape[0],1])

        print ("self.__min_distances", self.__min_distances.shape)
        # init clusters 3d kxNxL array (where k is the number of clusters, N is the number of samples, L is the number of classes/features if the input data)
        self.__clusters = np.zeros([self.__k,self.__unlabeled_data.shape[0],self.__unlabeled_data.shape[1]])
        print("self.__clusters", self.__clusters.shape)
        
        # a temp clusters 3d array
        self.__prev_clusters = np.zeros([self.__k,self.__unlabeled_data.shape[0],self.__unlabeled_data.shape[1]])
        print("self.__prev_clusters", self.__prev_clusters.shape)

        self.__clusters[:,:,:] = self.__unlabeled_data

        #print (self.__prev_clusters.shape)

    def load_data(self):
        """
        load the unlabeled data
        """
        self.__data = scipy.io.loadmat (self.__data_path + "/AllSamples.mat")
        self.__unlabeled_data = self.__data['AllSamples']

        if (self.__plot == True):
            colors = (0,0,0)
            area = np.pi*3
            self.__X1 = self.__unlabeled_data[:,[0]]
            self.__X2 = self.__unlabeled_data[:,[1]]
            plt.scatter(self.__X1, self.__X2,s=area,c=colors,alpha=0.5)
            plt.show()
            plt.title('Unlabled data scatter plot')
            plt.xlabel('X1')
            plt.ylabel('X2')
        
        if (self.__save == True):
            np.savetxt("data", self.__unlabeled_data,delimiter=",")
        


    def init_centeriods(self,cluster_init_strategy, k):
        if (cluster_init_strategy == 1):
            print("Cluster init strategy: randomally pick the initial canters from the givan examples ")
            # get list of random index in the range of the unlabled data
            rand_index = np.random.choice(self.__unlabeled_data.shape[0], self.__k, replace=False)
            # randomally init the centers lists
            self.__C = self.__unlabeled_data[rand_index]
            # a temp list of centers
            self.__C_prev = self.__C
            print("self.__C", self.__C)
        elif(cluster_init_strategy == 2):
            print("Cluster init strategy: first random. Other by min dustance")
        else:
            print ("Invalid cluster init strategy")

    # reshape the images for easier matrix operations
    def compute(self):
        
        self.init_centeriods(self.__cluster_init_strategy, self.__k)

        # here should the for loop according to the size of the rand_index array
        #print ("self.__distance = ", self.__distances.shape)
        #print(self.__distances.shape)
        #print(self.__distances[:,1,None].shape)

        #while()

        # get the distance from the cluster center/ loop here
        # for number of K's
        for k in range(0,self.__k):
            self.__distances[:,k,None] = np.linalg.norm(self.__clusters[k,:,:] - self.__C[k], axis=1, keepdims = True)
            #self.__distances[:,1,None] = np.linalg.norm(self.__clusters[1,:,:] - self.__C[1], axis=1, keepdims = True)   

        # argmin {distance_vector}
        self.__min_distances = np.argmin(self.__distances, axis=1)

        # save the last clusters
        self.__prev_clusters = self.__clusters

        # update the new clusters
        # For with number of K's
    
        for sample in self.__unlabeled_data:
            print(sample)
            #cluster_index = self.__min_distances[i]
            #print(cluster_index)
            #self.__clusters[cluster_index,:,:] = self.__prev_clusters[cluster]
            #self.__clusters[0,:,:] = self.__prev_clusters[]

        # calculate the new center


        #print(self.__distances.shape)
        if (self.__save == True):
            np.savetxt("distance0", self.__distances[:,0],delimiter=",")
            np.savetxt("distance1", self.__distances[:,1],delimiter=",")
            np.savetxt("min distances", self.__min_distances,delimiter=",")
        #print (self.__distances[:,1,None].shape)
        #print(self.__distances[:,1])
        #distance1 = np.linalg.norm(self.__unlabeled_data - C[0], axis=1,keepdims=True)
        #distance2 = np.linalg.norm(self.__unlabeled_data - C[1], axis=1,keepdims=True)
        
        #temp = np.argmin()
        #print (type(distance))
        #print (distance.shape)
        #np.savetxt("distance1", distance1,delimiter=",")
        #np.savetxt("distance2", distance2,delimiter=",")
        
        #print ("C : ", C)
        #print ("C_prev : ", C_prev)
        #print ("C_prev dims  = ", C_prev.shape)
        #error = np.linalg.norm(C-C_prev, axis = 1, keepdims=True)
        #print (error)
        #val = np.count_nonzero(error)
        #print(val)



    # show gray scale image
    def show_image(self,image):
        print ("show_image")
        #plt.imshow(image, cmap=plt.cm.gray)
        #plt.show()
        # calc the prior probabilities of getting digit 0 and digits 1
        # in our case the auumption is that they are equal to 0.5
        # 
        #     

def main():
    kMeansClustering = KMeansClustering()
    kMeansClustering.setup(1,2)
    kMeansClustering.compute()
    
    #kMeansClustering.setup(2,4)
    #kMeansClustering.compute()


if __name__ == "__main__":
    main()