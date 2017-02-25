from py2neo import Graph, Node, NodeSelector, Relationship
import requests,time

USERNAME = "neoneo"
PASSWORD = "neoneo"
IP = "192.168.0.111"
PORT = "7474"

class Neo(object):
	def __init__(self,base_url):
		self.base_url = base_url
		self.graph = Graph("http://%s:%s@%s:%s"%(USERNAME,PASSWORD,IP,PORT))
		self.sel = NodeSelector(self.graph)
		self.switches = {}
		self.hosts = {}

	def get_json(self,url):
		r = requests.get(url)
		s = str(r.text)
		return s

	def stringify(self,dic):		
		for key in dic.keys():
			dic[key] = str(dic[key])
		return dic

	def add_switches(self):
		url = self.base_url + "/switches"
		switch_array = eval(self.get_json(url))
		for switch_dict in switch_array:
			dpid = str(switch_dict['dpid'])
			switch_from_dict = self.switches.get(dpid,None)
			if switch_from_dict == None:				
				neo_node = self.sel.select('Switch',**self.stringify(switch_dict))
				neo_nodes = list(neo_node)
				
				if neo_nodes == []:
					print "Creating Switch: %s" % str(dpid)			
					node = Node('Switch',**self.stringify(switch_dict))
					self.graph.create(node)
					self.switches[dpid] = node
				else:
					self.switches[dpid] = neo_nodes[0]			


	def add_hosts(self):
		url = self.base_url + "/hosts"
		host_array = eval(self.get_json(url))
		for host_dict in host_array:			
			mac = host_dict['mac']
			host_from_dict = self.hosts.get(mac,None)
			if host_from_dict == None:
				neo_node = self.sel.select('Host',**self.stringify(host_dict))
				neo_nodes = list(neo_node)
				if neo_nodes == []:
					print "Creating Host: %s" % str(mac)
					node = Node('Host',**self.stringify(host_dict))
					self.graph.create(node)
					self.hosts[mac] = node
				else:
					self.hosts[mac] = neo_nodes[0]

	def add_host_links(self):
		url = self.base_url + "/hosts"
		host_array = eval(self.get_json(url))
		for host_dict in host_array:
			dpid = host_dict['port']['dpid']
			switch = self.switches[dpid]
			s_h_dict = {
				"src":host_dict['port'],
				"dst":host_dict['mac']
			}

			h_s_dict = {
				"src":host_dict['mac'],
				"dst":host_dict['port']
			}
			host = self.hosts[host_dict['mac']]
			rel = Relationship(switch,'S_H_LINK',host,**self.stringify(s_h_dict))
			self.graph.create(rel)

			rel = Relationship(host,'H_S_LINK',switch,**self.stringify(h_s_dict))
			self.graph.create(rel)

	def add_switch_links(self):
		url = self.base_url + "/links"
		link_array = eval(self.get_json(url))
		for link in link_array:
			src = link['src']
			dst = link['dst']
			node1 = self.switches[src['dpid']]
			node2 = self.switches[dst['dpid']]
			rel = Relationship(node1,"S_S_LINK",node2,**self.stringify(link))
			self.graph.create(rel)

	def push_to_neo(self,timestamp):
		self.timestamp = timestamp
		self.add_switches()
		self.add_switch_links()
		self.add_hosts()

if __name__ == '__main__':
	n = Neo("http://192.168.0.119:8080/v1.0/topology")
	n.push_to_neo(time.time())


