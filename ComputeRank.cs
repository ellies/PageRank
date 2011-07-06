using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;
using System.IO;

namespace siterank
{
    /**
     * Represents a site that docs belong to. 
     */
    public class Node
    {
        /** Site id or hostname  
         */
        public string name;
        /** Docs that belong to this site node
         */
        public List<int> docids = new List<int>();
        /** Stores nodes that point to this site node and # of site edges from them 
         */
        public Dictionary<Node, int> innodes = new Dictionary<Node, int>();
        /** Total number of incoming doc edges 
         */
        public double totalInDocCounts = 0;
        /** Stores nodes that this node points to and # of site edges to them 
         */
        public Dictionary<Node, int> outnodes = new Dictionary<Node, int>();
        /** Total number of outgoing doc edges 
         */
        public double totalOutDocCounts = 0;
        /** Site rank before next iteration 
         */
        public double oldrank = 0;
        /** Site rank after next iteration 
         */
        public double newrank = 0;
    }


    /** Create an IComparer for Node objects. 
     */
    class CompNode : IComparer<Node>
    {
        // Implement the IComparable interface. 
        //int Compare(object obj1, object obj2)
        public int Compare(Node a, Node b)
        {
            return -(a.newrank.CompareTo(b.newrank));
        }
    }

    /** Computes site rank for each node. 
    */
    public class ComputeRank
    {
        /** Threshold to determine site rank convergence has reached a stable point 
         */
        const double epsilon = 0.000001;
        /** Damping factor 
         */
        const double damping = 0.85;
        /** Floating point precision error allowed in computing total sum of site ranks  
         */
        const double margin = 0.05;
        /**  List of site nodes in the graph 
        */
        List<Node> nodes;
        /**  List of nodes without outgoing edges 
         */
        List<Node> deadends;
        /**  Store mapping between site name (site id or hostname) and site node
         */
        Dictionary<string, Node> sitenameToNode;
        /**  Store mapping between docid to site name 
         */
        Dictionary<int, string> idToSitename;
        /** True if site edges are used for computing site rank instead of doc edges 
         */
        bool collapsed = true;
        /** Compare nodes by siterank values 
         */
        CompNode comp;
        /** Part of file name.  E.g. "small2.txt" for a sample test
         */
        const string postfix = ".txt";
        /** Input file containing mapping between doc ids and site names
         */
        const string fileIdToSitename = @"input/idToSitename" + postfix;
        /** Input file containing mapping between doc ids and site urls (dislpay urls) of type SITE
         */
        const string fileIdToSite = "input/idToSite" + postfix;
        /** Input file containing mapping between source doc ids and target doc ids, both  site urls (access url) of type SITE
         */
        const string fileSrcToDst = "input/edges" + postfix;
        /** Output file containing doc ids and site ranks 
         */
        const string outfileIdToSiterank = @"output/idToSiterankCollapsed" + postfix;
        /** Output file containing root doc ids and site ranks 
         */
        const string outfileRootIdToSiterank = @"output/rootIdToSiterankCollapsed" + postfix;
        /** Number of docids to preallocate 
         */
        const int docidsCount = 63000000;
        /** FOR DEBUGGING 
         */
        const string logFile = @"output/logCollapsed" + postfix;
        static StreamWriter log = new StreamWriter(logFile);

        public ComputeRank()
        {
            sitenameToNode = new Dictionary<string, Node>();
            // preallocate to avoid outofmemory exception 
            idToSitename = new Dictionary<int, string>(docidsCount); 
            nodes = new List<Node>();
            deadends = new List<Node>();
            comp = new CompNode();
        }

        public static void Main()
        {
            try
            {
                DateTime t1 = DateTime.Now;
                ComputeRank cr = new ComputeRank();
                DEBUGPRINT(0, "1. Building a site graph...");
                DEBUGPRINT(0, "\tCreating nodes...");
                cr.createNodes();
                DEBUGPRINT(0, "\tCreating edges...");
                cr.createEdges();
                DEBUGPRINT(0, "2. Computing site rank...");
                cr.computeSiterank();
                DEBUGPRINT(0, "3. Writing site rank results...");
                cr.getSiteRanks();
                DEBUGPRINT(0, "End of program. Press any key to exit");
                DateTime t2 = DateTime.Now;
                DEBUGPRINT(0, "Everything took " + (t2 - t1).ToString() + " to complete");
                log.Close();
            }
            catch (Exception e)
            {
                DEBUGPRINT(0, e.Message);
                log.Close();
                throw;
            }

            Console.ReadKey(true);
        }

        /** Print for debugging in Console and log file
         */
        public static void DEBUGPRINT(int c, string s)
        {
            if (c % 20000000 == 0)
            {
                Console.WriteLine(s);
                log.WriteLine(s);
            }
        }

        /** Create a site graph 
         */
        public void createSiteGraph()
        {
            DEBUGPRINT(0, " Creating nodes...");
            DateTime t1 = DateTime.Now;
            createNodes();
            DateTime t2 = DateTime.Now;
            DEBUGPRINT(0, "Creating nodes took " + (t2 - t1).ToString() + " to complete");

            DEBUGPRINT(0, " Creating edges...");
            t1 = DateTime.Now;
            createEdges();
            t2 = DateTime.Now;
            DEBUGPRINT(0, "Creating edges took " + (t2 - t1).ToString() + " to complete");
        }


        /** Read each row in the input file and create a node for each unique site 
         */
        public void createNodes()
        {
            DateTime t1 = DateTime.Now;
            string line = "";
            string sitename = "";
            try
            {
                using (StreamReader sr = new StreamReader(fileIdToSitename))
                {
                    int i = 0;
                    while (!sr.EndOfStream)
                    {
                        i++;
                        line = sr.ReadLine();

                        //DEBUGPRINT(i, "\tReading: " + i + "th line");
                        int delimidx = line.IndexOf(",");
                        string strid = line.Substring(0, delimidx);
                        int docid = int.Parse(strid);
                        //DEBUGPRINT(i, "\tDoc id = " + docid);

                        sitename = line.Substring(delimidx + 1);
                        //DEBUGPRINT(i, "\tSite name = " + sitename);

                        createNode(docid, sitename);

                    }

                    DEBUGPRINT(0, "\tTotal #lines in " + fileIdToSitename + " = " + i);
                }
            }
            catch (Exception e)
            {
                DEBUGPRINT(0, e.Message + "\r\nRow = " + line);
                log.Flush();
                throw;
            }
            DEBUGPRINT(0, "Done creating all the nodes. There are " + nodes.Count + " nodes!");

            DateTime t2 = DateTime.Now;
            DEBUGPRINT(0, "Creating nodes took " + (t2 - t1).ToString() + " to complete");

        }

        /** Create a node for each unique site if it doesn't exist 
         */
        void createNode(int docid, string sitename)
        {
            if (!sitenameToNode.ContainsKey(sitename))
            {
                Node n = new Node();
                n.name = sitename;
                sitenameToNode.Add(sitename, n);
                nodes.Add(n);
            }
            sitenameToNode[sitename].docids.Add(docid); // Add doc id to this node 

            if (!idToSitename.ContainsKey(docid))  // docid must belong to one site 
            {
                idToSitename.Add(docid, sitenameToNode[sitename].name);
            }
        }


        /** Parse url to get a site id or a host name 
         */
        string parseUrl(string url)
        {
            try
            {
                string sitename = "";
                int idx = url.LastIndexOf("siteid=");  // Get the site id that appears last in the access url. 

                // There's no siteid so get the hostname and prepend it with a protocol 
                if (idx < 0)
                {
                    sitename = url.Substring(0, 4) + getHostName(url);
                }
                else
                {
                    // Get the site id in {....} 
                    int start = url.IndexOf("{", idx);
                    int end = url.IndexOf("}", start);
                    sitename = url.Substring(start + 1, end - start - 1);
                }

                return sitename;
            }
            catch (Exception e)
            {
                DEBUGPRINT(0, e.Message + " Wrong url format: " + url);
                log.Flush();
                throw;
            }
        }

        /** Parse url to get a host name 
         */
        string getHostName(string url)
        {
            //get hostname from url = http://hostname/abc/def/gh/

            int start = url.IndexOf("//");
            int end = url.IndexOf("/", start + 2);
            if (end < 0)
                end = url.Length;
            return url.Substring(start + 2, end - start - 2);
        }

        /** Create edges in the site graph 
         */
        public void createEdges()
        {
            DateTime t1 = DateTime.Now;

            using (StreamReader sr = new StreamReader(fileSrcToDst))
            {
                int i = 0;
                while (!sr.EndOfStream)
                {
                    // Each line contains source doc id and target doc id separated by a tab 
                    string line = sr.ReadLine();
                    i++;
                    //DEBUGPRINT(i, "\tReading " + i + "th line: " + line);                    
                    int spaceidx = line.IndexOf("\t");
                    string strsrc = line.Substring(0, spaceidx);
                    int srcid = int.Parse(strsrc);
                    string strdst = line.Substring(spaceidx + 1);
                    int dstid = int.Parse(strdst);
                    //DEBUGPRINT(i, "\tSrc id = " + srcid + ", Dst id = " + dstid);

                    try
                    {
                        // if source and target site nodes both exist 
                        if (idToSitename.ContainsKey(srcid) && idToSitename.ContainsKey(dstid))
                        {
                            string srcname = idToSitename[srcid];
                            string dstname = idToSitename[dstid];
                            // if source node name and target node name are different 
                            if (srcname != dstname)
                            {
                                Node srcnode = sitenameToNode[srcname];
                                Node dstnode = sitenameToNode[dstname];

                                //DEBUGPRINT(i, "\tSrc node = " + srcnode.name + ", Dst node = " + dstnode.name);

                                // Ensure there's an edge betweens src and target node and update the weight 
                                if (!dstnode.innodes.ContainsKey(srcnode))
                                {
                                    dstnode.innodes.Add(srcnode, 1);
                                }
                                else
                                {
                                    dstnode.innodes[srcnode]++;
                                }
                                dstnode.totalInDocCounts++;

                                if (!srcnode.outnodes.ContainsKey(dstnode))
                                {
                                    srcnode.outnodes.Add(dstnode, 1);
                                }
                                else
                                {
                                    srcnode.outnodes[dstnode]++;
                                }
                                srcnode.totalOutDocCounts++;
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        DEBUGPRINT(0, e.Message);
                        log.Flush();
                        throw;
                    }
                }
                DEBUGPRINT(0, "\tTotal #lines in file " + fileSrcToDst + " = " + i);
            }

            //For nodes that don't have any outgoing edges, we want to treat them as if they have outgoing edges to 
            //all the other nodes to simulate a user randomly choosing another node if the node is a dead-end. 
            foreach (Node nd in nodes)
            {
                if (nd.totalOutDocCounts == 0)
                {
                    deadends.Add(nd);
                }
            }
            DEBUGPRINT(0, "\tAdding dead-end nodes to a list...there are " + deadends.Count + " deadends!");

            DateTime t2 = DateTime.Now;
            DEBUGPRINT(0, "Creating edges took " + (t2 - t1).ToString() + " to complete");

        }


        /** Compute site rank per node 
         */
        public void computeSiterank()
        {
            DateTime t1 = DateTime.Now;

            /* 1. Initialize all nodes with equal rank = 1/N */
            int N = nodes.Count;
            DEBUGPRINT(0, "\tThere are total " + N + " nodes");
            IEnumerator<Node> ie = nodes.GetEnumerator();
            double initialrank = 1.0 / N;
            while (ie.MoveNext())
                ((Node)ie.Current).newrank = initialrank;
            DEBUGPRINT(0, "\tInitial rank = " + initialrank);

            /* Iteration steps */
            int c = 1;
            double maxdelta = 0;
            do
            {
                //DEBUGPRINT(c, "\r\n~~~~~~~~~~~~~~~~~~~~~~~Iteration " + c + "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
                maxdelta = 0;

                // 2. Save site rank in old rank 
                ie = nodes.GetEnumerator();
                while (ie.MoveNext())
                {
                    Node cur = ((Node)ie.Current);
                    cur.oldrank = cur.newrank;
                }

                // 3. Calculate the sum of contribution of every dead end node to every other node for this iteratoin 
                double deadendContribSum = 0;
                foreach (Node dead in deadends)
                    deadendContribSum += (dead.oldrank / (N - 1));
                //DEBUGPRINT(c, "Sum of contribution of a deadend node to other nodes = " + deadendContribSum);

                if (deadendContribSum > 1)
                {
                    DEBUGPRINT(0, "Sum of contribution of a deadend node should never be > 1!");
                    log.Flush();
                    throw new Exception("Sum of contribution of a deadend node should never be > 1!");
                }

                // 4. For each node pointing to this node, add the source node's contribution based on its site rank and the weight of the connection 
                ie = nodes.GetEnumerator();
                while (ie.MoveNext())
                {
                    Node cur = ((Node)ie.Current);
                    //DEBUGPRINT(c, "PR[" + cur.name + "]: ");

                    double sum = 0;
                    foreach (Node nd in cur.innodes.Keys)
                    {
                        if (cur != nd)
                        {
                            double factor = collapsed ? (1.0 / nd.outnodes.Count) : (cur.innodes[nd] * 1.0 / nd.totalOutDocCounts);
                            double inputContrib = nd.oldrank * factor;
                            //DEBUGPRINT(c, "\tContribution from " + nd.name + " = " + inputContrib);
                            // Add the contribution of normal (non-deadend) input nodes 
                            sum += inputContrib;
                        }
                    }

                    // 5. If the current node is a deadend, then subtract its contribution from the total sum of deadend contribution because it shouldn't contribute to itself
                    double updatedDeadCSum = deadendContribSum;
                    if (cur.totalOutDocCounts == 0)
                        updatedDeadCSum -= (cur.oldrank / (N - 1));
                    //DEBUGPRINT(c, "\tUpdated sum of deadend rank contribution = " + updatedDeadCSum);

                    // 6. Add both contributions 
                    sum += updatedDeadCSum;
                    //DEBUGPRINT(c, "\tSum of both = " + sum);
                    if (sum > 1)
                    {
                        DEBUGPRINT(0, "Sum of normal and deadend input node contribution should never be > 1!");
                        log.Flush();
                        throw new Exception("Sum of normal and deadend input node contribution should never be > 1!");
                    }
                    // 7. Apply damping factor 
                    cur.newrank = ((1 - damping) / N) + (damping * sum);
                    //DEBUGPRINT(c, "     = (1-d)/N + d * " + sum + " = " + cur.newrank);

                    // 8. Calculate the max change between the new rank and the saved rank before the next iteration, and compare against epsilon  
                    double diff = Math.Abs(cur.newrank - cur.oldrank);
                    if (maxdelta < diff)
                        maxdelta = diff;
                }

                checkNormalization(c);
                //DEBUGPRINT(c, "Maxdelta = " + maxdelta + ", Epsilon = " + epsilon);
                c++;

            } while (maxdelta > epsilon);  // 9. Iterate until convergence reached a stable point 

            DateTime t2 = DateTime.Now;
            DEBUGPRINT(0, "Computing siterank took " + (t2 - t1).ToString() + " to complete");

            getIterationSummary(c - 1);
        }

        /** Reports top 10 (or nodes.count) site ranks, total # of iterations/nodes/deadends, and sum of site ranks
         */
        void getIterationSummary(int iter)
        {
            DEBUGPRINT(0, "\r\n########### SUMMARY ##############");
            nodes.Sort(comp);

            int count = nodes.Count > 10 ? 10 : nodes.Count;
            int i = 0;
            DEBUGPRINT(0, "Top " + count + " site ranks:");
            while (i < count)
            {
                int indocscount = 0;
                Node nd = nodes[i];

                // DELETE THIS LATER 
                foreach (Node innode in nd.innodes.Keys)
                    indocscount += nd.innodes[innode];

                DEBUGPRINT(0, "PR[" + nd.name + "] = " + nd.newrank
                              + "\r\n\t#Docs for site = " + nd.docids.Count
                              + "\r\n\t#Incoming site edges = " + nd.innodes.Count
                              + "\r\n\t#Outgoing site edges = " + nd.outnodes.Count
                              + "\r\n\t#Incoming doc edges = " + indocscount
                              + "\r\n\t#Incoming doc edges (totalINdocs) = " + nd.totalInDocCounts
                              + "\r\n\t#Outgoing doc edges = " + nd.totalOutDocCounts);
                i++;
            }

            double totalSiteEdges = 0;
            double totalDocEdges = 0;
            foreach (Node nd in nodes)
            {
                totalDocEdges += nd.totalOutDocCounts;
                totalSiteEdges += nd.outnodes.Count;
            }

            DEBUGPRINT(0, "Total #Iterations = " + iter);
            DEBUGPRINT(0, "Total #Nodes = " + nodes.Count);
            DEBUGPRINT(0, "Total #Deadends = " + deadends.Count);
            DEBUGPRINT(0, "Total #Site Edges = " + totalSiteEdges);
            DEBUGPRINT(0, "Total #Doc Edges = " + totalDocEdges);
            checkNormalization(0);
        }

        /** Checks if the sum of site ranks is 1 
         */
        void checkNormalization(int c)
        {
            if (c % 5000 == 0)
            {
                double total = 0;
                foreach (Node nd in nodes)
                    total += nd.newrank;

                DEBUGPRINT(0, "Sum of site ranks = " + total);

                if (total > 1 + margin || total < 1 - margin)
                {
                    log.Close();
                    throw new Exception("Sum of site ranks is not 1!! It is " + total + "!");
                }
            }
        }

        /** Outputs site rank result to a file 
         */
        public void getSiteRanks()
        {
            DateTime t1 = DateTime.Now;

            StreamWriter outIdToSiterank = new StreamWriter(outfileIdToSiterank);
            foreach (Node nd in nodes)
            {
                foreach (int id in nd.docids)
                    outIdToSiterank.WriteLine(id + ", " + nd.newrank);
            }
            outIdToSiterank.Close();

            StreamWriter outRootIdToSiterank = new StreamWriter(outfileRootIdToSiterank);
            using (StreamReader sr = new StreamReader(fileIdToSite))
            {
                int i = 0;
                while (!sr.EndOfStream)
                {
                    // Each line contains source doc id and target doc id separated by a tab 
                    string line = sr.ReadLine();
                    i++;
                    //DEBUGPRINT(i, "\tReading " + i + "th line: " + line);

                    int spaceidx = line.IndexOf("\t");
                    if (spaceidx > 0)
                    {
                        string strid = line.Substring(0, spaceidx);
                        int docid = int.Parse(strid);
                        //DEBUGPRINT(i, "\tDoc id = " + docid);

                        string dispurl = line.Substring(spaceidx + 1); // spaceidx+1 must be < line.length
                        //DEBUGPRINT(i, "\tDisplay url = " + dispurl);

                        string site = idToSitename[docid];
                        Node node = sitenameToNode[site];

                        outRootIdToSiterank.WriteLine(docid + ", " + site + ", " + dispurl + ", " + node.newrank + ", " + node.innodes.Count + ", " + node.outnodes.Count + ", " + node.totalInDocCounts + ", " + node.totalOutDocCounts);
                    }
                }

                outRootIdToSiterank.Close();
            }

            DateTime t2 = DateTime.Now;
            DEBUGPRINT(0, "Writing siterank results took " + (t2 - t1).ToString() + " to complete");
        }
    }
}
