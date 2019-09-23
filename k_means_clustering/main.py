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
        self.__kmeans_algo_iterations = 0

        if (data_dir_path is None):
            # assume data dir is in the current directory
            self.__data_path = os.path.dirname(os.path.realpath(__file__))
            # make sure required file are in the data_dir_path
            for mat_file in self.__data_file_list:
                if (False == os.path.isfile(mat_file)):
                    print ("Missing file : " + str(mat_file))


    def cleanup(self):
        self.__unlabeled_data=[]
        self.__distances = []
        self.__clusters = []
        self.__cluster_init_strategy = 1
        self.__k = 2
        self.__kmeans_algo_iterations = 0

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
        self.__cluster_init_strategy = cluster_init_strategy

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
        
        if (self.__save == False):
            np.savetxt("data", self.__unlabeled_data,delimiter=",")
        


    def init_centeriods(self,cluster_init_strategy, k):
        if (cluster_init_strategy == 1):
            print("Cluster init strategy: randomally pick the initial canters from the givan examples ")
            # get list of random index in the range of the unlabled data
            rand_index = np.random.choice(self.__unlabeled_data.shape[0], self.__k, replace=False)
            # randomally init the centers lists
            self.__C = self.__unlabeled_data[rand_index]
            self.__C_old = np.zeros(self.__C.shape)
            
        elif(cluster_init_strategy == 2):
            print("Cluster init strategy: first random. Other centers by the average of the maximal distance")
            # Pick the first center randomally
            # I am picking them all randomally, and override (according to the description below) starting the second center
            rand_index = np.random.choice(self.__unlabeled_data.shape[0], self.__k, replace=False)
            self.__C = self.__unlabeled_data[rand_index]
            self.__C_old = np.zeros(self.__C.shape)

            for x in range(0,self.__unlabeled_data.shape[0]):
                sample = self.__unlabeled_data[x]
                if sample[0] < 0:
                    print ("sample[0]<0")
                    print(sample[0])
                if sample[1] < 0:
                    print ("sample[1]<0")
                    print(sample[1])
            # override center array from position 1
            # For the i-th center (i>1), choose a sample (among all possible samples)
            # such that the average distance of this chosen one to all previous (i-1)
            # centers is maximal
            flag = True
            if (flag == True):
                n = 0
                max_distance = 0
                sample = []
                candidate_sample = []
                distance = 0

                for k in range (1,self.__k):                         
                    # iterate all samples       
                    for x in range(0,self.__unlabeled_data.shape[0]):
                        # get a sample
                        sample = self.__unlabeled_data[x]
                        # iterate over the previous centers
                        distance = 0
                        n = 0
                        for j in range (0,k):
                            n = n + 1
                            # sum the distance of the current sample from the previous clusters centers
                            distance = distance + np.linalg.norm(sample - self.__C[j],keepdims = True)
                            #print("Distance = ", distance, "Sample = ", sample, "center =", self.__C[j])

                        # average the previous distances
                        # make sure n is not zero
                        average_distance = distance / n
                        if (average_distance > max_distance):
                            # update the max value and and candidate sample
                            candidate_sample = sample
                            max_distance = average_distance
                    # update the final centers 
                    self.__C[k] = candidate_sample
                    max_distance = 0
                    sample = []
                    candidate_sample = []


            print (self.__C)
        else:
            print ("Invalid cluster init strategy")

    
    def compute(self):
        
        self.init_centeriods(self.__cluster_init_strategy, self.__k)
             
        while(np.allclose(self.__C,self.__C_old) == False):
            self.__kmeans_algo_iterations = self.__kmeans_algo_iterations + 1

            self.__C_old = np.copy(self.__C)

            # get the distance from the cluster center
            for k in range(0,self.__k):
                self.__distances[:,k,None] = np.linalg.norm(self.__unlabeled_data - self.__C[k], axis=1, keepdims = True)
                        
            self.__min_distances = np.argmin(self.__distances, axis=1)
            
            for k in range (0,self.__k):
                cluster_index = np.where(self.__min_distances == k)
                cluster = np.take(self.__unlabeled_data,cluster_index,axis=0)
                self.__C[k] = np.mean(cluster, axis=1)            

            if (self.__save == True):
                np.savetxt("distance0", self.__distances[:,0],delimiter=",")
                np.savetxt("distance1", self.__distances[:,1],delimiter=",")
                np.savetxt("min distances", self.__min_distances,delimiter=",")

        print ("Number of clusters = ", self.__k)
        print ("Number of Kmeans iterations = ", self.__kmeans_algo_iterations)
        print ("Clusters centroid = ", self.__C)

        self.cleanup()

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
    kMeansClustering.setup(2,6)
    kMeansClustering.compute()
    
    #kMeansClustering.setup(2,4)
    #kMeansClustering.compute()


if __name__ == "__main__":
    main()