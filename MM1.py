#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import random
import csv
import copy
import matplotlib.pyplot as plt
import numpy as np

LAMBDAS = [0.2, 0.4, 0.6, 0.8, 0.9]
#LAMBDAS = [0.2, 0.4, 0.6]
JOB_SIZE = 10
TOTAL_SIMULATION_TIME = 10000
DEBUG = False  # True for detailed logs
CSV = True

# classe que cria um objeto que representa o trabalho
class Job:
    def __init__(self, arrival_time, job_id, job_size):
        self.job_id = job_id
        self.arrival_time = arrival_time
        self.service_time = random.expovariate(1 / job_size)
        self.service_start_time = 0
        self.service_end_time = 0
        self.job_delay_time = 0
        self.queue_time = 0
        self.status = 0  # 0 for created, 1 for queued, 2 for processing, 3 for completed

    def add_and_process_job_queue(self, this_system):
        self.service_time = self.service_time
        self.service_start_time = max(self.arrival_time, this_system.latest_job_service_ending_time)
        self.service_end_time = self.service_start_time + self.service_time
        self.queue_time = self.service_start_time - self.arrival_time
        self.job_delay_time = self.queue_time + self.service_time

class System:
    def __init__(self, service_rate):
        self.service_rate = service_rate  # random.expovariate(self.service_rate)
        self.latest_job_service_ending_time = 0  # initially no job
        self.queue_list = []
        self.queue_summary_over_time = {}

    def handle_jobs(self, the_new_job):
        current_time = the_new_job.arrival_time
        self.latest_job_service_ending_time = the_new_job.service_end_time

        new_job_inserted = False
        finished_jobs = []
        temp_copy_of_jobs_in_sys = copy.copy(self.queue_list)

        for this_job in temp_copy_of_jobs_in_sys:
            if this_job.service_start_time <= current_time and this_job.status < 2:
                if DEBUG:
                    print("Time: " + str(this_job.service_start_time) + "secs \t\tJod Id: " + str(
                        this_job.job_id) + " Started processing ..... ")
                self.queue_summary_over_time[current_time] = len(self.queue_list)
                this_job.status = 2
                if this_job.service_end_time <= current_time:
                    this_job.status = 3
                    if DEBUG:
                        print("Time: " + str(this_job.service_end_time) + "secs \t\tJod Id: " + str(
                            this_job.job_id) + " Finished processing , queue size is: " + str(len(self.queue_list) - 1))
                    self.queue_list.remove(this_job)
                    self.queue_summary_over_time[current_time] = len(self.queue_list)
                    finished_jobs.append(this_job)
                else:
                    continue

            elif this_job.service_end_time <= current_time and this_job.status == 2:
                this_job.status = 3
                if DEBUG:
                    print("Time: " + str(this_job.service_end_time) + "secs \t\tJod Id: " + str(
                        this_job.job_id) + " Finished processing, queue size is: " + str(len(self.queue_list) - 1))
                self.queue_list.remove(this_job)
                self.queue_summary_over_time[current_time] = len(self.queue_list)
                finished_jobs.append(this_job)

        if not new_job_inserted:
            # add current job to the system's jobs
            self.queue_list.append(the_new_job)
            # update queue summary
            self.queue_summary_over_time[current_time] = len(self.queue_list)

            if DEBUG:
                print("Time: " + str(current_time) + "secs \t\tJod Id: " + str(
                    the_new_job.job_id) + " Entered system, system job size is: " + str(len(self.queue_list)))
            the_new_job.status = 1

    def finalize_jobs(self):
        temp_copy_of_jobs_in_sys_at_end_time = copy.copy(self.queue_list)
        current_time = TOTAL_SIMULATION_TIME

        for this_job in temp_copy_of_jobs_in_sys_at_end_time:
            if this_job.status == 2:
                this_job.status = 3
                if DEBUG:
                    print("Time: " + str(this_job.service_end_time) + "ses \t\tJod Id: " + str(
                        this_job.job_id) + " Finished processing, queue size is: " + str(len(self.queue_list) - 1))
                self.queue_list.remove(this_job)
                self.queue_summary_over_time[this_job.service_end_time] = len(self.queue_list)
                if this_job.service_end_time > current_time:
                    current_time = this_job.service_end_time
            elif this_job.status < 2:
                if DEBUG:
                    print("Time: " + str(this_job.service_start_time) + "secs \t\tJod Id: " + str(
                        this_job.job_id) + " Started processing ..... ")
                self.queue_summary_over_time[this_job.service_end_time] = len(self.queue_list)
                this_job.status = 2

                this_job.status = 3
                if DEBUG:
                    print("Time: " + str(this_job.service_end_time) + "secs \t\tJod Id: " + str(
                        this_job.job_id) + " Finished processing , queue size is: " + str(len(self.queue_list) - 1))
                self.queue_list.remove(this_job)
                self.queue_summary_over_time[this_job.service_end_time] = len(self.queue_list)
                if this_job.service_end_time > current_time:
                    current_time = this_job.service_end_time

        print("Time: " + str(current_time) + "secs End of last job in the System\nSimulation summary:")

class Simulator:
    def __init__(self, arrival_rate, service_rate):
        self.arrival_rate = arrival_rate
        self.system = System(service_rate)

    def run(self, simulation_time):
        disciplina = 'FCFS'
        print(f'\nSimulação com disciplina {disciplina.upper()} λ = {self.arrival_rate}')
        current_time = random.expovariate(self.arrival_rate)
        this_jobs = {}  # map of id:job
        job_id = 1
        lista = []

        while current_time <= simulation_time:
            new_job = Job(current_time, job_id, self.system.service_rate)
            this_jobs[job_id] = new_job
            new_job.add_and_process_job_queue(self.system)
            self.system.handle_jobs(new_job)
            current_time += random.expovariate(self.arrival_rate)
            job_id += 1

        self.system.finalize_jobs()
        print("Total jobs: " + str(len(this_jobs)))
        return this_jobs

def plot_simulation_delay_time_per_job(jobs, arrival_rate, sumarize):
    job_ids = [key for key in jobs]

    simulation_data = [job_ids, [jobs[job_id].job_delay_time for job_id in jobs]]

    simulation_delay_avg = sum(simulation_data[1]) / len(simulation_data[1])
    print("Average delay per job: " + str(simulation_delay_avg))
    simulation_data_delay_averages = [job_ids, [simulation_delay_avg for job_id in jobs]]

    theoretical_data = [job_ids, [1 / ((1 / JOB_SIZE) - arrival_rate) for job_id in jobs]]

    plt.figure(" Figure for lambda=" + str(arrival_rate))
    this_axis = plt.subplot()
    this_axis.step(simulation_data[0], simulation_data[1], label='Simulation delay time per job id')
    this_axis.step(simulation_data_delay_averages[0], simulation_data_delay_averages[1], label='Simulation E[T]')
    this_axis.step(theoretical_data[0], theoretical_data[1], label='Theoretical E[T]')
    this_axis.set_xlabel('Job Id')
    this_axis.set_ylabel('Delay Time (secs)')
    this_axis.legend()
    this_axis.set_title(
        "Delay time M/M/1 λ=" + str(arrival_rate) + " Simulation time: " + str(TOTAL_SIMULATION_TIME) + " secs")
    sumarize[arrival_rate] = [simulation_delay_avg, 1 / ((1 / JOB_SIZE) - arrival_rate)]
    #plt.savefig('plt_' + str(arrival_rate) + '.png')

def plot_grafico(lista, disciplina, saida='plot', x_legenda='x', y_legenda='y'):
    fig = plt.figure()
    y = np.arange(len(lista))
    plt.plot(y, lista, linewidth=0.5, c='k', label='lista')
    plt.title(disciplina.upper())
    plt.grid()
    fig.savefig(saida + str(this_lambda) + '.png')
    plt.close()

if __name__ == '__main__':
    summary_results = {}
    for this_lambda in LAMBDAS:
        simulator = Simulator(this_lambda, JOB_SIZE)
        the_jobs = simulator.run(TOTAL_SIMULATION_TIME)

        plot_simulation_delay_time_per_job(the_jobs, this_lambda, summary_results)

        lista1 = [] # média de tempo de chegada
        lista2 = [] # média do tempo ocupado
        lista3 = [] # média do tempo de serviço
        # write data to csv file
        if CSV:
            outDataFileName = "./jobs_" + str(this_lambda) + ".csv"
            outfile = open(outDataFileName, "w")
            writer = csv.writer(outfile)
            header = ['id', 'arrival', 'service time start', 'service time end', 'total delay', 'Service time',
                      'queue time']
            writer.writerow(header)
            for job in the_jobs:
                job_data = []
                job_data.append(the_jobs[job].job_id)
                job_data.append(the_jobs[job].arrival_time)
                job_data.append(the_jobs[job].service_start_time)
                job_data.append(the_jobs[job].service_end_time)
                job_data.append(the_jobs[job].job_delay_time)
                job_data.append(the_jobs[job].service_time)
                job_data.append(the_jobs[job].queue_time)
                writer.writerow(job_data)

                lista1.append(the_jobs[job].arrival_time/the_jobs[job].job_id)
                lista2.append(the_jobs[job].service_time/the_jobs[job].job_id)
                lista3.append(the_jobs[job].queue_time/the_jobs[job].job_id)
                
            plot_grafico(lista1, 'FCFS', 'plot1')
            plot_grafico(lista3, 'FCFS', 'plot3')
            plot_grafico(lista2, 'FCFS', 'plot2')
            outfile.close()
    
    lamdas = [lamda for lamda in summary_results]

    the_simulation_data = [lamdas, [summary_results[lamda][0] for lamda in lamdas]]
    the_theoretical_data = [lamdas, [summary_results[lamda][1] for lamda in lamdas]]

    plt.figure(" Comparison")
    axis = plt.subplot()

    plt.plot(the_simulation_data[0], the_simulation_data[1], 'b--')
    plt.plot(the_simulation_data[0], the_simulation_data[1], 'bs', label='Simulation delay time')

    plt.plot(the_theoretical_data[0], the_theoretical_data[1], 'g--')
    plt.plot(the_theoretical_data[0], the_theoretical_data[1], 'go', label='Theoretical E[T]')

    axis.set_xlabel('Lambda Value')
    axis.set_ylabel('Delay Time (secs)')
    axis.legend()
    axis.set_title("Simulation Vs steady-state: Avg Delay time on M/M/1 " + ", Simulation time: " + str(
        TOTAL_SIMULATION_TIME) + "secs")
    #plt.show()
    #plt.savefig('plot.png')
    plt.close()
