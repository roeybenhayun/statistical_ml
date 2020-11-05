import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
import numpy as np
from kMeansClustering import KMeansClustering
from clustering_datasets import datasets
from DBSCAN import DBSCAN

def main():

    ds = "jain"
    dbscan = DBSCAN(datasets[ds]["path"],datasets[ds]["name"], datasets[ds]["format"],plot_dataset=True)

    return

    dataset = "a2"

    one_shot = False
    calc_sse = True
    run_scikit_kmeans = False
    number_of_clusters = 30
    

    kMeansClustering = KMeansClustering(datasets[dataset]["path"],datasets[dataset]["name"], datasets[dataset]["format"],plot_dataset=False)
        
    if (one_shot == True):        
        kMeansClustering.setup(KMeansClustering.K_MEANS_PLUS_PLUS,number_of_clusters)
        kMeansClustering.compute(plot_centroids = True)
        kMeansClustering.get_sse()
        kMeansClustering.cleanup()
    
    if (run_scikit_kmeans == True):
        unlabled_data = kMeansClustering.get_dataset()
        kmeans = KMeans(n_clusters=14, verbose=0, init='random').fit(unlabled_data)
        print("scikit kmeans clusters" , kmeans.cluster_centers_)
        X1 = unlabled_data[:,[0]]
        X2 = unlabled_data[:,[1]]  
        plt.scatter(X1, X2,c='b',s=np.pi*3, alpha=0.5)              
        plt.title('Clustering using scikitlearn')
        plt.xlabel('X1')
        plt.ylabel('X2') 
        plt.scatter(kmeans.cluster_centers_[:,[0]],kmeans.cluster_centers_[:,[1]],s=100,c='r',alpha=0.5) 
        plt.show(block=True)
        #plt.pause()
        #plt.close()



    # Finding the number of clusters using Elbow Method
    if (calc_sse == True):
        number_of_runs = 1
        k = np.arange(2,number_of_clusters+1)
        start_cluster = 2
        plot_objective_function = True
        objective_function = []
        kMeansClustering.cleanup()

        for run in range(0, number_of_runs):
            for _k in range(start_cluster,number_of_clusters+1):
                kMeansClustering.setup(KMeansClustering.RANDOM_INIT,_k)
                kMeansClustering.compute(plot_centroids = False)
                objective_function.append(kMeansClustering.get_sse())
                kMeansClustering.cleanup()

            if (plot_objective_function == True):
                plt.title('Elbow Graph')
                plt.xlabel('Number of Clusters K')
                plt.ylabel('Objective Function Value')
                label = "Strategy 1 - run" + str(run)
                plt.plot (k,objective_function, c='b', label = label)
                plt.legend()
                plt.show()
            
            objective_function = []

if __name__ == "__main__":
    main()