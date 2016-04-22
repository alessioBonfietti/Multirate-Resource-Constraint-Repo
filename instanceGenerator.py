#!/usr/bin/env python
#


import json
import sys
import random
import os
from numpy.random import beta, poisson
from math import ceil, pi, exp


class Architecture(object):
	
	def __init__(self):
		self.cores = []
		self.resources = []
		self.availability = []
		self.interCoreBitrate = 1
		self.interCoreDelay = 0
		self.interDeviceBitrate = 1
		self.interDeviceDelay = 0  
		
	@property
	def n_cores(self):
		return len(self.cores)   
	
class Parameters(object):
	
	def __init__(self):
		self.maxTaskDuration = 1
		self.maxApps = 1
		self.maxTasks = 1
		self.maxOccupation = 1
		self.minOccupation = 1
		self.maxTaskDuration = 1
		self.minTaskDuration = 1
		self.maxPeriodFactor = 2
		self.alpha = 1
		self.beta = 1
		self.epsilon = 0
		self.wMin = 0
		self.wMax = 0
		self.consMin = 0
		self.consMax = 0
		self.forkDegree = 0
		self.joinDegree = 0
		
class Task(object):

	def __init__(self):
		self.nameAttribute = None
		self.inPort = []
		self.outPort = []
		self.id = None
		self.durationsCum = []
		self.durationScra = 0
		self.cons = []

class Edge(object):

	def __init__(self):
		self.nameAttribute = None
		self.src = None
		self.snk = None
		self.size = None

class Application(object):

	def __init__(self):
		self.nameAttribute = None
		self.tasks = []
		self.priority = 0
		self.edges = []
		self.period = 0
		self.schedulable = True
		
	@property
	def n_tasks(self):
		return len(self.tasks)
	
	@property
	def n_edges(self):
		return len(self.edges)
		
	def genGraphFile(self, fileName):
		f = open(fileName, "w")
		f.write("digraph G {\n")
		for e in self.edges:
			f.write(str(e.src)+ " -> "+ str(e.snk)+ ";\n")
		f.write("}")
		f.close()
		os.system('dot -Tpdf  ' +fileName+' -O 2>/dev/null')

			
class Instance(object):
	
	def __init__(self):
		self.arch = None
		self.apps = []
		
	def __str__(self):
		out = "INST: "+str(len(self.apps))
		#out += "C:"
		#for core in self.arch.cores:
		#	out += " " + core
		if len(self.arch.resources) > 0: 
			out += "\nRH:" 
			for resource in self.arch.resources:
				out += " " + str(resource)
			out += "\nRV:"
			for av in self.arch.availability:
				out += " " + str(av)
#		out += "\ninterCoreBitrate: " + str(self.arch.interCoreBitrate) + "\n"
#		out += "interCoreDelay: " + str(self.arch.interCoreDelay) + "\n"
#		out += "interDeviceBitrate: " + str(self.arch.interDeviceBitrate) + "\n"
#		out += "interDeviceDelay: " + str(self.arch.interDeviceDelay) + "\n"
		out += "\ninterCoreCommunicationBitrate: " + str(self.arch.interCoreBitrate) + "\n"
		out += "interCoreCommunicationDelay: " + str(self.arch.interCoreDelay) + "\n"
		out += "interruptLatency: 4" + "\n"
		for app in self.apps:
			out += "PR: " + str(app.priority) +"\n"
			out += "GH: dline nres" 
			for core in self.arch.cores:
				out += " " + core
			out += "\n"
			
			out += "GV: " + str(app.period) +" " + str(len(self.arch.cores))
			for core in self.arch.cores:
				out += " 0"
			out += "\n"
			if len(self.arch.resources) > 0:
				out += "CH:"
				for resource in self.arch.resources:
					out += " " + str(resource)
				for t in app.tasks:
					out += "\nCV:"
					for cons in t.cons:
						out += " " + str(cons)
				out+= "\n"
			if len(self.arch.resources) > 0:
				out += "NH: FPGA"
			else:
				out += "NH:"
			for c in self.arch.cores: out += " " +c 
			out += "\n"
			for t in app.tasks:
				out += "NV:" 
				if len(self.arch.resources) > 0:
					out+=" " + str(t.durationScra)
				for i in range(self.arch.n_cores):
					out += " " + str(t.durationsCum[i])
				out += "\n"
			out += "AH: src dst size\n"
			for e in app.edges:
				out += "AV: " + str(e.src) + " " + str(e.snk) + " " + str(e.size) + "\n"
			out += "END\n"
		return out
	
	@property
	def n_apps(self):
		return len(self.apps) 
		
	
def parseInstance(filename):
	arch = Architecture()
	apps = []
	inst = Instance()
	with open(filename) as f:
		elements = f.readline().split(" ")
		for i in range(1,len(elements)):
			arch.cores.append(elements[i].strip())
		elements = f.readline().split(" ")
		for i in range(1, len(elements)):
			arch.resources.append(elements[i].strip())
		elements = f.readline().split(" ")
		for i in range(1, len(elements)):
			arch.availability.append(elements[i].strip())
		elements = f.readline().split(" ")
		arch.interCoreBitrate = int(elements[1])
		elements = f.readline().split(" ")
		arch.interCoreDelay = int(elements[1])
		elements = f.readline().split(" ")
		arch.interDeviceBitrate = int(elements[1])
		elements = f.readline().split(" ")
		arch.interDeviceDelay = int(elements[1])
		elements = f.readline().split(" ")
		while len(elements) == 2 and elements[0] == "PR:":
			app = Application()
			app.priority = int(elements[1])
			app.period = int(f.readline().split(" ")[1])
			#skip Resources header
			f.readline()
			elements = f.readline().split(" ")
			while elements[0] == "CV:":
				task = Task()
				for j in range(1, len(elements)):
					task.cons.append(int(elements[j]))
				elements = f.readline().split(" ")
				app.tasks.append(task)
			#read first TV line
			elements = f.readline().split(" ")
			t = 0
			while elements[0] == "NV:":
				app.tasks[t].durationScra = int(elements[1])
				for j in range(2, len(elements)):
					app.tasks[t].durationsCum.append(int(elements[j]))
				elements = f.readline().split(" ")
				t += 1
			#read first GV line
			elements = f.readline().split(" ")
			while elements[0] == "AV:":
				edge = Edge()
				edge.src = int(elements[1])
				edge.snk = int(elements[2])
				edge.size = int(elements[3])
				app.edges.append(edge)
				elements = f.readline().split(" ")
			apps.append(app)
			#skip END
			elements = f.readline().split(" ")
		inst.arch = arch
		inst.apps = apps
		
	return inst

def parseParameters(fileName):
	
	data = json.loads(open(fileName).read())
	
	arch_data = data["architecture"]
	arch = Architecture()
	arch.cores = arch_data["cores"]
	arch.resources = arch_data["resources"]
	arch.availability = arch_data["availability"]
	arch.interCoreBitrate = arch_data["interCoreBitrate"]
	arch.interCoreDelay = arch_data["interCoreDelay"]
	arch.interDeviceBitrate = arch_data["interDeviceBitrate"]
	arch.interDeviceDelay = arch_data["interDeviceDelay"]
	
	params_data = data["parameters"]
	params = Parameters()
	params.maxApps = params_data["maxApps"]
	params.maxTaskDuration = params_data["maxTaskDuration"]
	params.maxOccupation = params_data["maxOccupation"]
	params.minOccupation = params_data["minOccupation"]
	params.maxTaskDuration = params_data["maxTaskDuration"]
	params.minTaskDuration = params_data["minTaskDuration"]
	params.maxPeriodFactor = params_data["maxPeriodFactor"]
	params.alpha = params_data["alpha"]
	params.beta = params_data["beta"]
	params.epsilon = params_data["epsilon"]
	params.wMin = params_data["wMin"]
	params.wMax = params_data["wMax"]
	params.consMin = params_data["consMin"]
	params.consMax = params_data["consMax"]
	params.forkDegree = params_data["forkDegree"]
	params.joinDegree = params_data["joinDegree"]

	return arch, params


def genGraph(params, numTasks):
	graph = []
	for i in range(numTasks):
		graph.append([])
		for j in range(numTasks):
			graph[i].append(0)
	edges = []
	fronteer = [0]
	nodes = range(1, numTasks)
	last = 1
	# print "in"
	while nodes:
		newFronteer = []

		for f in fronteer:
			n = min(poisson(params.forkDegree - 1) + 1, len(nodes))
			for i in range(n):
				graph[f][last] = 1
				e = Edge()
				e.src = f
				e.snk = last
				e.size = 0
				if((params.wMax - params.wMin) >0):
					e.size = random.choice(range(params.wMin, params.wMax))
				edges.append(e)
				newFronteer.append(last)
				nodes.remove(last)
				last += 1
		fronteer = newFronteer
	# print "half"
	for j in range(numTasks-1):
		n = min(poisson(params.joinDegree - 1), numTasks-i-1)
		for i in range(n):
			sink = int(ceil((numTasks - j - 1) * beta(0.8, numTasks))) + j

			while graph[j][sink] == 1 and sink < numTasks - 1:
				sink += 1
			e = Edge()
			e.src = j
			e.snk = sink
			e.size = 0
			if((params.wMax - params.wMin) >0):
				e.size = random.choice(range(params.wMin, params.wMax))			
			edges.append(e)
			graph[j][sink] = 1
	# print "out"
	return edges


def genAllApplications(arch, params):
	applications = []
	#TODO set the smallest period be configurable by input
	period = 30#medium instance
	#period = 50 #hard instance
	#period = int(ceil(params.maxTaskDuration / ((params.maxOccupation+params.minOccupation)/2))*10) #parametric way
	priority = 1
	totalDur = 0
	stop = False
	while(not stop):
		
		occ = random.uniform(params.minOccupation, params.maxOccupation)
		occ -= (occ* 0.05 *len(applications))
		app = Application()
		app.priority = priority
		app.period = period
		appDur = 0
		while float(appDur) / period < occ:
			t = Task()
			dur = int(ceil((params.maxTaskDuration - params.minTaskDuration) * beta(params.alpha, params.beta))) + params.minTaskDuration
			appDur += dur
			for j in range(len(arch.cores)):
				t.durationsCum.append(dur)
			t.durationScra = int(ceil(dur*(1+random.uniform(-params.epsilon,params.epsilon))))
			for k in range(len(arch.availability)):
				t.cons.append(int(ceil(arch.availability[k]*random.uniform(params.consMin, params.consMax))))
			app.tasks.append(t)
		totalDur += appDur
		
		if totalDur / period / arch.n_cores < params.maxOccupation:
			app.edges = genGraph(params, app.n_tasks)
			applications.append(app)
			priority += 1
			factor = params.maxPeriodFactor#random.choice(range(int(ceil(params.maxPeriodFactor/2)), params.maxPeriodFactor+1))
			if factor <2:
				factor = 2
			totalDur *= factor
			period *= factor
		else:
			stop = True
		
		if len(applications) >= params.maxApps:
			stop = True
		
	return applications
	
def generate(param, output, graph = False):
	(a, p) = parseParameters(param)
	ok = False
	apps = []
	while not ok:
		ok = True
		apps = genAllApplications(a, p)
		for i in apps:
			if len(i.tasks) < 2 :
				ok = False
	inst = Instance()
	inst.arch = a
	inst.apps = apps

	# print inst	
	outFile = open(output, "w")
	outFile.write(str(inst))
	outFile.close()
	if(graph):
		for i,f in enumerate(inst.apps):
			f.genGraphFile(output+"."+str(i)+".dot")


if __name__ == '__main__':
	(a, p) = parseParameters(sys.argv[1])
	apps = genAllApplications(a, p)
	inst = Instance()
	inst.arch = a
	inst.apps = apps

	#print inst	
	outFile = open(sys.argv[2], "w")
	outFile.write(str(inst))
	outFile.close()
	for i,f in enumerate(inst.apps):
		f.genGraphFile(sys.argv[2]+"."+str(i)+".dot")
	
