

#include <stdio.h>

#include <string.h>

#include <math.h>

#include"GraphLite.h"



#define VERTEX_CLASS_NAME(name) GraphClor##name



#define EPS 1e-6

#include <list>

#include <algorithm>


#include<time.h>

#define random(x) (rand()%x)



using namespace std;

int color_num=0;

int vo_id=0;





class VERTEX_CLASS_NAME(InputFormatter): public InputFormatter {

public:

    int64_t getVertexNum() {

        unsigned long long n;

        sscanf(m_ptotal_vertex_line, "%lld", &n);

        m_total_vertex= n;

        return m_total_vertex;

    }

    int64_t getEdgeNum() {

        unsigned long long n;

        sscanf(m_ptotal_edge_line, "%lld", &n);

        m_total_edge= n;

        return m_total_edge;

    }

    int getVertexValueSize() {

        m_n_value_size = sizeof(int);

        return m_n_value_size;

    }

    int getEdgeValueSize() {

        m_e_value_size = sizeof(int);

        return m_e_value_size;

    }

    int getMessageValueSize() {

        m_m_value_size = sizeof(int);

        return m_m_value_size;

    }

    void loadGraph() {

        unsigned long long last_vertex;

        unsigned long long from;

        unsigned long long to;

        double weight = 0;

        

        int value = -1;

        int outdegree = 0;

        

        const char *line= getEdgeLine();



        // Note: modify this if an edge weight is to be read

        //       modify the 'weight' variable



        sscanf(line, "%lld %lld", &from, &to);

        addEdge(from, to, &weight);



        last_vertex = from;

        ++outdegree;

        for (int64_t i = 1; i < m_total_edge; ++i) {

            line= getEdgeLine();



            // Note: modify this if an edge weight is to be read

            //       modify the 'weight' variable



            sscanf(line, "%lld %lld", &from, &to);

            if (last_vertex != from) {

                addVertex(last_vertex, &value, outdegree);

                last_vertex = from;

                outdegree = 1;

            } else {

                ++outdegree;

            }

            addEdge(from, to, &weight);

        }

        addVertex(last_vertex, &value, outdegree);

    }

};



class VERTEX_CLASS_NAME(OutputFormatter): public OutputFormatter {

public:

    void writeResult() {

        int64_t vid;

        int value;

        char s[1024];



        for (ResultIterator r_iter; ! r_iter.done(); r_iter.next() ) {

            r_iter.getIdValue(vid, &value);

            int n = sprintf(s, "%lld: %d\n", (unsigned long long)vid, value);

            writeNextResLine(s, n);

        }

    }

};



// An aggregator that records a double value tom compute sum

class VERTEX_CLASS_NAME(Aggregator): public Aggregator<double> {

public:

    void init() {

        m_global = 0;

        m_local = 0;

    }

    void* getGlobal() {

        return &m_global;

    }

    void setGlobal(const void* p) {

        m_global = * (double *)p;

    }

    void* getLocal() {

        return &m_local;

    }

    void merge(const void* p) {

        m_global += * (double *)p;

    }

    void accumulate(const void* p) {

        m_local += * (double *)p;

    }

};



class VERTEX_CLASS_NAME(): public Vertex <int, int, int> {

public:

    void compute(MessageIterator* pmsgs) {

        int val;

        if (getSuperstep() == 0) {

						if(vo_id==getVertexId())

						{

							val=0;//produce color  for the fisrt point  

							 * mutableValue() = val; //to color the first vo  

							sendMessageToAllNeighbors(val);//send his own color to neighbors  

							voteToHalt(); return;

						}

						

        } else {









							vector<int> color_list_message;

							bool is_contain=false;	

							int current_color=getValue();//to save current vetex's color	

							int cnt = 0;									

						for ( ; ! pmsgs->done(); pmsgs->next() ) {//save the message into color_list_message for the late deal  

									int color= pmsgs->getValue();

									color_list_message.push_back(color);	

									if(color==current_color)	

									{

										is_contain=true;

									}		

									cnt++;					

									

							}



						if(cnt ==0)

						{

							voteToHalt(); return;

						}





						if(!is_contain&&current_color!=-1)//to judge whether the color of neighbors is the same color of own

						{

							voteToHalt(); return;

						}						

					/*	vector<int>::iterator Itor_messsage;

						vector<int>::iterator Itor;

						for ( Itor_messsage = color_list_message.begin(); Itor_messsage != color_list_message.end();Itor_messsage++ )

						{

							for ( Itor = color_list.begin(); Itor != color_list.end();Itor++ )

										{

											 if ( *Itor == *Itor_messsage )

												{          

													Itor = color_list.erase(Itor);

												}

												

										}

						}	*/

					bool is_yes=true;

					int	r;

					vector<int>::iterator Itor_messsage;
					struct timespec time1 = {0, 0};   
      
   			 clock_gettime(CLOCK_REALTIME, &time1);
						srand(time1.tv_nsec);

					while(is_yes)

				{



							r = random(color_num);

						for ( Itor_messsage = color_list_message.begin(); Itor_messsage != color_list_message.end();Itor_messsage++ )

						{

								if(r==*Itor_messsage)

								{is_yes=true;break;}

								is_yes=false;

						}

            



				}

						 * mutableValue() =r;

						 sendMessageToAllNeighbors(r);//send his own color to neighbors 

							voteToHalt(); return;

            

            }



          

    }

};





class VERTEX_CLASS_NAME(Graph): public Graph {

public:

    VERTEX_CLASS_NAME(Aggregator)* aggregator;



public:

    // argv[0]: PageRankVertex.so

    // argv[1]: <input path>

    // argv[2]: <output path>

    void init(int argc, char* argv[]) {



        setNumHosts(5);

        setHost(0, "localhost", 1411);

        setHost(1, "localhost", 1421);

        setHost(2, "localhost", 1431);

        setHost(3, "localhost", 1441);

        setHost(4, "localhost", 1451);



        if (argc < 3) {

           printf ("Usage: %s <input path> <output path>\n", argv[0]);

           exit(1);

        }



        m_pin_path = argv[1];

        m_pout_path = argv[2];



        //aggregator = new VERTEX_CLASS_NAME(Aggregator)[1];

        //regNumAggr(1);

        //regAggr(0, &aggregator[0]);



		sscanf(argv[3],"%d",&vo_id);

		sscanf(argv[4],"%d",&color_num);

	

    }



    void term() {

        //delete[] aggregator;

    }

};



/* STOP: do not change the code below. */

extern "C" Graph* create_graph() {

    Graph* pgraph = new VERTEX_CLASS_NAME(Graph);



    pgraph->m_pin_formatter = new VERTEX_CLASS_NAME(InputFormatter);

    pgraph->m_pout_formatter = new VERTEX_CLASS_NAME(OutputFormatter);

    pgraph->m_pver_base = new VERTEX_CLASS_NAME();



    return pgraph;

}



extern "C" void destroy_graph(Graph* pobject) {

    delete ( VERTEX_CLASS_NAME()* )(pobject->m_pver_base);

    delete ( VERTEX_CLASS_NAME(OutputFormatter)* )(pobject->m_pout_formatter);

    delete ( VERTEX_CLASS_NAME(InputFormatter)* )(pobject->m_pin_formatter);

    delete ( VERTEX_CLASS_NAME(Graph)* )pobject;

}