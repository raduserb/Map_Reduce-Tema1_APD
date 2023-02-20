#include <iostream>
#include <pthread.h>
#include <string.h>
#include <fstream>
#include <algorithm>
#include <map>
#include <vector>
#include <math.h>
#include <unistd.h>

using namespace std;

pthread_barrier_t barrier;
int q,m;

struct my_file{
    string name;
    int size;
};

struct argument{
    int type; // 0 => mapper, 1 => reducer
    int id;
    int reducer_nr;
    int mapper_nr;
    vector <string> file_names;
};

bool binarySearch(int n, int l, int r, int i)
{
    if (r >= l) {
        int mid = l + (r - l) / 2;
 
        long res = 1;
        for(int j = 0 ; j < i ; j++)
        {
            res *= mid;
            if(res > n)
                return binarySearch(n, l, mid - 1, i );
        }
        if(res == n)
            return 1;
 
        return binarySearch(n, mid + 1, r, i);
    }
    return 0;
}


void *f(void *arg) {
    struct argument* f_arg = static_cast<struct argument*> (arg);
    int i, j, nr;
    string line;
    static vector <int> vec[4][6];
        
    if((f_arg->type) == 0)// map
    {
        for (auto it = f_arg->file_names.begin(); it != f_arg->file_names.end(); it++) 
        {
            ifstream in_file(*it);
            getline(in_file,line);
            nr = stoi(line);

            vector <int> numbers;
            for(j = 0 ; j < nr ; j++)
            {
                getline(in_file,line);
                int n = stoi(line);
                numbers.push_back(n);
            }
            in_file.close();

            for(i = 2 ; i < f_arg->reducer_nr + 2; i++)
            {
                for(j = 0 ; j < nr ; j++)
                {
                    if(numbers[j] == 1)
                        vec[f_arg->id][i].push_back(numbers[j]);
                    
                    if( binarySearch(numbers[j], 2, numbers[j], i) )
                        vec[f_arg->id][i].push_back(numbers[j]);
                }
            }
        }
    }
    
    q = pthread_barrier_wait(&barrier);

    if((f_arg->type ) == 1)//reduce
    {
        vector <int> my_list;
        for( j = 0 ; j < f_arg->mapper_nr ; j++)
        {
            for (auto it = vec[j][f_arg -> id].begin();it != vec[j][f_arg -> id].end(); it++) 
            {
                if ( find(my_list.begin(), my_list.end(), *it) == my_list.end()) 
                {
                    my_list.push_back(*it);
                }
            }
        }
        ofstream MyFile( "out" + to_string(f_arg -> id)+ ".txt");
        MyFile<<my_list.size();
        MyFile.close();
    }

    pthread_exit(NULL);
}

bool compare_file_size(my_file a, my_file b) //functia de sortare a fisierelor in functie de size
{
    return (a.size < b.size);
}

int main(int argc, char **argv){

    int mapper_nr, reducer_nr, files_nr, id, r;
    mapper_nr = atoi(argv[1]);
    reducer_nr = atoi(argv[2]);
    ifstream in_file(argv[3]);

    struct my_file files[3005];
    struct argument arg[mapper_nr+reducer_nr];
    pthread_t threads[mapper_nr + reducer_nr];
    
    string line;
    getline(in_file,line);
    files_nr = stoi(line);
    
    for( id = 0; id < files_nr ; id++)
    {
        getline(in_file,line);
        files[id].name = line;
        ifstream test_file(line);
        test_file.seekg(0, ios::end);
        files[id].size = test_file.tellg();
        test_file.close();
    }

    in_file.close();

    sort(files, files + files_nr, compare_file_size);
 
    for ( id = 0 ; id < files_nr; id++)
        arg[id%mapper_nr].file_names.push_back( files[id].name);
    

    for(id = 0 ; id< mapper_nr + reducer_nr;id++)
    {
        arg[id].reducer_nr = reducer_nr;
        arg[id].mapper_nr = mapper_nr;
    }


    q = pthread_barrier_init(&barrier, NULL, mapper_nr + reducer_nr);

    if(q!= 0)
    {
        printf("eroare\n");
        return 0;
    }

    for (id = 0; id < mapper_nr + reducer_nr; id++) 
    {
		if(id < mapper_nr)
        {
            arg[id].type = 0;
            arg[id].id = id;
	        r = pthread_create(&threads[id], NULL, f, &arg[id]);
        }
        else
        {
            arg[id].type = 1;
            arg[id].id = id - mapper_nr + 2;
            r = pthread_create(&threads[id], NULL, f, &arg[id]);
        }
        
		if (r) 
        {
	  		printf("Eroare la crearea thread-ului %d\n", id);
	  		exit(-1);
		}
  	}

    for ( id = 0; id < mapper_nr + reducer_nr; id++) 
    {
        r = pthread_join(threads[id],NULL);
        if (r) {
            printf("Eroare la asteptarea thread-ului %d\n", id);
            exit(-1);
        }
    }
    
    q = pthread_barrier_destroy(&barrier);

    if(q!= 0)
    {
        printf("eroare\n");
        return 0;
    }
    
  	pthread_exit(NULL);
}