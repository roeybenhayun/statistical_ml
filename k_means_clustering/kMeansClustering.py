import scipy.io
import matplotlib.pyplot as plt
import numpy as np
import os
import matplotlib.pyplot as plt
import math
from tempfile import TemporaryFile

class KMeansClustering:    
    RANDOM_INIT=1
    K_MEANS_PLUS_PLUS=2
    def __init__(self, dataset_path=None, dataset=None, format = None, plot_dataset=False):
        self.__dataset_path = dataset_path
        self.__data_path = dataset_path
        self.__dataset = dataset 
        self.__data_file_list = [dataset_path + "/" + dataset]
        self.__print_dims = False
        self.__plot_dataset = plot_dataset
        self.__plot_centroids = False
        self.__save = False
        self.__log_enabled = False
        self.__unlabeled_data=[]
        self.__distances = []
        self.__clusters = []
        self.__cluster_init_strategy = 1
        self.__k = 2
        self.__kmeans_algo_iterations = 0
        self.__final_clusters = []
        

        if (dataset_path is None):
            # assume data dir is in the current directory
            self.__data_path = os.path.dirname(os.path.realpath(__file__))
            # make sure required file are in the dataset_path
            for mat_file in self.__data_file_list:
                if (False == os.path.isfile(mat_file)):
                    print ("Missing file : " + str(mat_file))
                    return
        
        self.load_data(format)


    def cleanup(self):
        """
        Cleanup and set default values. This is private method which is called on exit of compute method
        Parameters
        ----------
        None

        Return
        ------
        None
        """
        self.__distances = []
        self.__clusters = []
        self.__cluster_init_strategy = 1
        self.__k = 2
        self.__kmeans_algo_iterations = 0

    def setup(self,cluster_init_strategy, k):
        """
        Initial setup and input params validation for the KMeans Clustering algorithm. 
        This is a 'private' method. This method should be invoked internally
        Parameters
        ----------
        cluster_init_strategy, the selection used for the initial cluster center
                1 - randomally pick the initial canters from the givan examples (default is 1)
                2 - pick the first randomally
        k, number of clusters (2-10) (default is 2)

        Return
        ------
        None     
        """
        if (cluster_init_strategy != 1 and cluster_init_strategy !=2):
            print ("Invalid cluster selection strategy. Please supported method")
            return
        if ( k<2 or k>100):
            print ("Invalid number of clusters selection. Supported range is 2-10.")
            return
        
        self.__k = k
        self.__cluster_init_strategy = cluster_init_strategy
                
        # init distance 2d Nxk array (where N is the number of samples and k is the number of clusters)
        self.__distances = np.zeros([self.__unlabeled_data.shape[0],k])

        self.__min_distances = np.zeros([self.__unlabeled_data.shape[0],1])

        # init clusters 3d kxNxL array (where k is the number of clusters, N is the number of samples, L is the number of classes/features if the input data)
        self.__clusters = np.zeros([self.__k,self.__unlabeled_data.shape[0],self.__unlabeled_data.shape[1]])
        
        # a temp clusters 3d array
        self.__prev_clusters = np.zeros([self.__k,self.__unlabeled_data.shape[0],self.__unlabeled_data.shape[1]])

        self.__clusters[:,:,:] = self.__unlabeled_data

        if (self.__log_enabled == True):
            print("self.__distances", self.__distances.shape)
            print ("self.__min_distances", self.__min_distances.shape)
            print("self.__clusters", self.__clusters.shape)
            print("self.__prev_clusters", self.__prev_clusters.shape)


        


    def load_data(self,format):
        """
        Load the input data (e.g. the samples to be clusters)
        Parameters
        ----------
        None

        Return
        ------
        None     
        """     
        area = np.pi*3   
        if (format == "mat"):
            self.__data = scipy.io.loadmat (self.__data_path + "/" + self.__dataset)
            self.__unlabeled_data = self.__data['AllSamples']
            self.__X1 = self.__unlabeled_data[:,[0]]
            self.__X2 = self.__unlabeled_data[:,[1]]            
            if (self.__plot_dataset == True):                                
                plt.scatter(self.__X1, self.__X2,s=area,c='b',alpha=0.5)              
                plt.title(self.__dataset)
                plt.xlabel('X1')
                plt.ylabel('X2')       
                plt.show()
        elif (format == "txt"):
            self.__data = np.loadtxt(self.__data_path + "/" + self.__dataset)                        
            self.__unlabeled_data = self.__data
            self.__X1 = self.__unlabeled_data[:,[0]]
            self.__X2 = self.__unlabeled_data[:,[1]]  
            if (self.__plot_dataset == True):  
                plt.scatter(self.__X1, self.__X2,s=area,c='b',alpha=0.5)              
                plt.title(self.__dataset)
                plt.xlabel('X1')
                plt.ylabel('X2')       
                plt.show()


        if (self.__save == True):
            np.savetxt("data", self.__unlabeled_data,delimiter=",")
        

    def get_dataset(self):
        return self.__unlabeled_data


    def init_centeriods(self,cluster_init_strategy, k):

        """
        Init clusters centroid according to init strategy and the number of clusters
        Parameters
        ----------
        cluster_init_strategy, the selection used for the initial cluster center
                1 - randomally pick the initial canters from the givan examples (default is 1)
                2 - pick the first randomally
        k, number of clusters (2-10) (default is 2)

        Return
        ------
        None     
        """
        if (cluster_init_strategy == KMeansClustering.RANDOM_INIT):
            if (self.__log_enabled == True):
                print("Cluster init strategy: randomally pick the initial canters from the givan examples ")
            # get list of random index in the range of the unlabled data
            rand_index = np.random.choice(self.__unlabeled_data.shape[0], self.__k, replace=False)
            # randomally init the centers lists
            self.__C = self.__unlabeled_data[rand_index]
            self.__C_old = np.zeros(self.__C.shape)
            
        elif(cluster_init_strategy == KMeansClustering.K_MEANS_PLUS_PLUS):
            if (self.__log_enabled == True):
                print("Cluster init strategy: first random. Other centers by the average of the maximal distance")
            # Pick the first center randomally
            # I am picking them all randomally, and override (according to the description below) starting the second center
            self.__C_old = np.zeros([self.__k,self.__unlabeled_data.shape[1]])
            self.__C = np.copy(self.__C_old)

            rand_index = np.random.choice(self.__unlabeled_data.shape[0], 1, replace=False)
            self.__C[0] = self.__unlabeled_data[rand_index]
            

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
                            l2_norm = np.linalg.norm(sample - self.__C[j],keepdims = True)
                            squared_l2_norm = l2_norm * l2_norm
                            distance = distance + squared_l2_norm
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

        else:
            print ("Invalid cluster init strategy")

    
    def compute(self, plot_centroids = False):

        """
        The implementation of the k means algorithem.        
        1. init centeroids
        2. calc the distance for the centeroids
        3. get the minimum distance to the centroid
        4. calc the mean
        5. update the new centroid untill algo converge (the error vector is 0)

        Parameters
        ----------
        None

        Return
        ------
        None     
        """ 
        area = np.pi*3
        # @todo - remove, not needed
        self.__X1 = self.__unlabeled_data[:,[0]]
        self.__X2 = self.__unlabeled_data[:,[1]]  

        self.init_centeriods(self.__cluster_init_strategy, self.__k)
         
        error = np.linalg.norm(self.__C - self.__C_old, axis=1, keepdims = True)

        while(error.any() != 0):
            if (plot_centroids == True):
                plt.scatter(self.__X1, self.__X2,s=area,c='b',alpha=0.5)              
                plt.title('Unlabled data scatter plot')
                plt.xlabel('X1')
                plt.ylabel('X2') 
                plt.scatter(self.__C[:,[0]],self.__C[:,[1]],s=100,c='r',alpha=0.5) 
                plt.show(block=False)
                plt.pause(1)
                plt.close()
                

            self.__kmeans_algo_iterations = self.__kmeans_algo_iterations + 1

            self.__C_old = np.copy(self.__C)

            self.__final_clusters = []

            # get the distance from the Kth cluster center
            for k in range(0,self.__k):
                l2_norm = np.linalg.norm(self.__unlabeled_data - self.__C[k], axis=1, keepdims = True)
                self.__distances[:,k,None] = l2_norm
            # get in indices of the min distance fron the Kth cluster        
            self.__min_distances = np.argmin(self.__distances, axis=1)

            for k in range (0,self.__k):
                cluster_index = np.where(self.__min_distances == k)
                cluster = np.take(self.__unlabeled_data,cluster_index,axis=0)
                
                self.__final_clusters.append(cluster)

                # !!NOTE --> there are some cases when running the kmeans algo with initializaion method 2 
                # that the INITIAL centroid list has an identical centers. e.g.
                # [[ 1.77775261  7.21854537]
                # [ 6.5807212  -0.0766824 ]
                # [ 9.26998864  9.62492869] <---
                # [ 3.85212146 -1.08715226]
                # [ 2.95297924  9.65073899]
                # [ 9.26998864  9.62492869]] <--
                # in that case np.take will return an empty list. Since there are an identical centers, the algo
                # skips the calculation for that point
                if (cluster.size == 0):continue
                # update cluster centroids
                self.__C[k] = np.mean(cluster, axis=1)            


            if (self.__log_enabled == True):
                print ("C = ",self.__C, "C_old = ", self.__C_old)

            # calc distance of current centroids to previous censtroids (while loop will be break when there will no 
            # change in distance)
            error = np.linalg.norm(self.__C - self.__C_old, axis=1, keepdims = True)

            if (self.__save == True):
                np.savetxt("distance0", self.__distances[:,0],delimiter=",")
                np.savetxt("distance1", self.__distances[:,1],delimiter=",")
                np.savetxt("min distances", self.__min_distances,delimiter=",")

        if (self.__log_enabled == True):
            print ("Number of clusters = ", self.__k)
            print ("Number of Kmeans iterations = ", self.__kmeans_algo_iterations) 
        
    def get_sse(self):
        # raise an exception if final clusters are empty
        # call this method once computed method has completed and the centroids have been caluculated
        # @todo - plot final clustering
        sum_of_squered_distances = 0
        for k in range (0,self.__k):
            cluster = self.__final_clusters[k]               
            l2_norm = np.linalg.norm(cluster[0,:,:] - self.__C[k], axis=1, keepdims = True)
            sse = l2_norm * l2_norm
            sum_of_squered_distances = sum_of_squered_distances + np.sum(sse)                                    
        return sum_of_squered_distances

    #def get_seperation(self):
    #    seperation = 0
    #    data_mean = np.mean(self.__unlabeled_data,axis=0)
    #    for k in range (0,self.__k):
    #        cluster = self.__final_clusters[k]
    #        seperation += cluster.shape[1]* np.square(self.__C[k] - data_mean)
    #    return seperation        
