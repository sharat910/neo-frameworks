from py2neo import Graph, Node, NodeSelector, Relationship
import time

USERNAME = "neoneo"
PASSWORD = "neoneo"
IP = "192.168.0.111"
PORT = "7474"

class Neo(object):
	def __init__(self):
		self.net = None
		self.timestamp = None
		self.graph = Graph("http://%s:%s@%s:%s"%(USERNAME,PASSWORD,IP,PORT))
		self.sel = NodeSelector(self.graph)
		self.switches = {}
		self.hosts = {}

	def node_check(self,new_net):
		if self.net == None:
			return True
		old_nodes = self.net.nodes()
		new_nodes = new_net.nodes()
		if len(old_nodes) != len(new_nodes):
			return True

		if sorted(old_nodes) == sorted(new_nodes):
			return False
		else:
			return True

	def node_creator(self,nodes):
		print "Creating nodes.."
		hosts = filter(lambda x: len(str(x)) == 17,nodes)
		switches = filter(lambda x: len(str(x)) != 17,nodes)
		print hosts,switches
		for switch_id in switches:
			neo_node = self.sel.select('Switch',id = str(switch_id))
			neo_nodes = list(neo_node)
			if neo_nodes == []:
				print "nodehere"
				node = Node('Switch',id = str(switch_id))
				self.switches[str(switch_id)] = node
				self.graph.create(node)
			elif self.switches.get(neo_nodes[0]['id'],None) == None:
				self.switches[neo_nodes[0]['id']] = neo_nodes[0]

		for host_mac in hosts:
			neo_node = self.sel.select('Host',mac = str(host_mac))
			neo_nodes = list(neo_node)
			if neo_nodes == []:
				print "hosthere"
				node = Node('Host',mac = str(host_mac))
				self.hosts[str(host_mac)] = node
				self.graph.create(node)
			elif self.hosts.get(neo_nodes[0]['mac'],None) == None:
				self.hosts[neo_nodes[0]['mac']] = neo_nodes[0]
	
	def create_relations(self,links,link_type):
		print "Creating relations.."
		print self.switches
		print self.hosts
		for link in links:
			if link_type == "S_S_LINK":
				node1 = self.switches[str(link[0])]
				node2 = self.switches[str(link[1])]
			elif link_type == "S_H_LINK":
				node1 = self.switches[str(link[0])]
				node2 = self.hosts[str(link[1])]
			else:
				node1 = self.hosts[str(link[0])]
				node2 = self.switches[str(link[1])]

			rel = Relationship(node1,link_type,node2)
			for key in link[2]:
				rel[key] = link[2][key]
			rel['timestamp'] = self.timestamp
			self.graph.create(rel)
			
	def push_to_neo(self,net,timestamp):
		print "\n\n\n Pushing data \n\n\n"
		print self.node_check(net)
		if self.node_check(net):
			self.node_creator(net.nodes())
			self.net = net
		
		links = self.net.edges(data=True)
		sslinks = filter(lambda x: x[2].get("delay",None) != None,links)		
		hlinks = filter(lambda x: x[2].get("delay",None) == None,links)
		shlinks = filter(lambda x: x[2].get("port",None) != None,hlinks)
		hslinks = filter(lambda x: x[2].get("port",None) == None,hlinks)

		self.timestamp = timestamp
		self.create_relations(sslinks,'S_S_LINK')
		self.create_relations(shlinks,'S_H_LINK')
		self.create_relations(hslinks,'H_S_LINK')


	def create_multiple_relations(self,links,link_type):
		print "Creating relations.."

		def fun(x):
			return str(x).replace("'","")

		for link in links:

			link[2].update({'timestamp':self.timestamp})

			if link_type == 'SS':
				self.graph.run("""
				MATCH (n1:Switch {id:'%s'}), (n2:Switch {id:'%s'})
				CREATE (n1)-[:S_S_LINK%s]->(n2)
				"""%(tuple(map(fun,link))))

			elif link_type == 'SH':
				self.graph.run("""
				MATCH (n1:Switch {id:'%s'}), (n2:Host {mac:'%s'})
				CREATE (n1)-[:S_H_LINK%s]->(n2)
				"""%(tuple(map(fun,link))))

			else:			
				self.graph.run("""
				MATCH (n1:Host {mac:'%s'}), (n2:Switch {id:'%s'})
				CREATE (n1)-[:H_S_LINK%s]->(n2)
				"""%(tuple(map(fun,link))))
