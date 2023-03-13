/*
 * Author: Hongtao Zhang
 * An Inter-Thread Publisher Subscriber Pattern (ITPS) C++ implementation using boost library
 */



#pragma once
#include <iostream>
#include <string>
#include <unordered_map>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/thread/thread.hpp>
#include <boost/signals2.hpp>
#include <exception>
#include "CpQueue.hpp"


/* Synchronization for Reader/Writer problems */
// get exclusive access
#define ITPS_writer_lock(mutex) do { \
    boost::upgrade_lock<boost::shared_mutex> __writer_lock(mutex); \
    boost::upgrade_to_unique_lock<boost::shared_mutex> __unique_writer_lock( __writer_lock ); \
}while(0)
// get shared access
#define ITPS_reader_lock(mutex) boost::shared_lock<boost::shared_mutex>  __reader_lock(mutex); 



#define AVOID_STARVATION_DELAY 1 // unit: microseconds


// class ChannelAlreadyRegisteredException: public std::exception
// {
//   virtual const char* what() const throw()
//   {
//     return "Channel Already Registered";
//   }
// };


// Inter-Thread Publisher-Subscriber System
namespace ITPS {


    /*
     * 
     *  * (NonBlocking Mode)Trivial Mode: msg channel only use 1 field to store the msg, every time a new msg
     *      is sent from the publisher, the field gets overwritten, hence only the latest msg
     *      is saved.
     *  * (Blocking Mode)Message Queue Mode: use a MQ to store a series of msgs, MQ is instantiated by subscriber
     *      Each subscriber gets its own MQ. When MQ is full, the publisher thread is suspended until
     *      the queue is consumed(pop) by a subscriber to give room for new msgs. Check cp_queue.hpp 
     *      for implementation details of the consumer-producer queue.
     *      
     *  * MsgChannel utilizes a HashTable to manage messgaes
     *  * publisher instantiates a MsgChannel
     *  * subscriber contains constructors to instantiate a message queue
     * 
     *  * One important distinction between Trivial Mode and MQ Mode:
     *      * Trivial mode's subcriber's getter function "Msg getMsg(void)" 
     *        is non-blocking, and might get garbage value when publisher hasn'Msg published anything
     *      * MQ mode's getter function "Msg getMsg(void)" is conditionally blocking, when
     *        the queue is empty, getter's thread gets blocked until something is published into the queue.
     *        Similarly, when the queue is full, the publisher is blocked instead. 
     */


//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /* This class serves as a bridge between the Publisher class and the Subcribe class*/ 
    template<typename Msg>
    class MsgChannel {
        protected:

            // unordered map == hash map
            typedef std::unordered_map<std::string, MsgChannel<Msg>*> msg_table_t;
        public:

            MsgChannel(std::string topic_name, std::string msg_name, std::string mode) {
                ITPS_writer_lock(table_mutex);
                this->key = topic_name + "." + msg_name + "." + mode;
                // std::cout << key << std::endl;
                
                // if key doesn't exist
                if(msg_table.find(key) == msg_table.end()) {
                    msg_table[key] = this;
                } else {
                    throw "ChannelAlreadyRegisteredException";
                }
            }

            static MsgChannel *get_channel(std::string topic_name, std::string msg_name, std::string mode) {
                ITPS_reader_lock(table_mutex);
                std::string key = topic_name + "." + msg_name + "." + mode;
                
                // if key doesn'Msg exist
                if(msg_table.find(key) == msg_table.end()) {
                    return nullptr;
                }
                return msg_table[key];
            }

            void add_msg_queue(boost::shared_ptr<ConsumerProducerQueue<Msg>> queue) {
                ITPS_writer_lock(msg_mutex); 
                msg_queues.push_back(queue);
            }


            /* deprecated, no longer integrate observer pattern into this implementation
            void add_slot(boost::function<void(Msg)> callback_function) {
                callback_funcs.push_back(callback_function);
            }*/

            // Non-blocking Mode
            void set_msg(Msg msg) {
                ITPS_writer_lock(msg_mutex);
                message = msg;
            }

            // Non-blocking Mode
            Msg get_msg() { 
                ITPS_reader_lock(msg_mutex);
                return this->message;
            }
            
            // Blocking Mode
            void enqueue_msg(Msg msg) {
                ITPS_writer_lock(msg_mutex);
                
                /* enqueue MQ */
                for(auto& queue: msg_queues) {
                    queue->produce(msg); // it will block the publisher's thread if the queue is full
                }
            }

            // Blocking Mode
            void enqueue_msg(Msg msg, unsigned int timeout_ms) {
                ITPS_writer_lock(msg_mutex);
                /* timed enqueue MQ */
                for(auto& queue: msg_queues) {
                    queue->produce(msg, timeout_ms); // if timed out, it won'Msg block
                }
            }


        protected:
            Msg message;
            static msg_table_t msg_table;
            
            boost::shared_mutex msg_mutex;
            static boost::shared_mutex table_mutex;

            std::string key;

            std::vector< boost::shared_ptr<ConsumerProducerQueue<Msg>> > msg_queues;
            std::vector< boost::function<void(Msg)> > callback_funcs;
    };
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////





//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    template <typename Msg>
    class Publisher {
        public:
            Publisher(std::string topic_name, std::string msg_name, std::string mode) {
                // if two publisher uses the same topic_nam + msg_name + mode, they would share the same msg channel 
                try {
                    channel = boost::shared_ptr<ITPS::MsgChannel<Msg>>(
                        new ITPS::MsgChannel<Msg>(topic_name, msg_name, mode));
                } catch(const char* e) {
                    if(std::string(e) == "ChannelAlreadyRegisteredException") {
                        channel = boost::shared_ptr<ITPS::MsgChannel<Msg>>(
                            ITPS::MsgChannel<Msg>::get_channel(topic_name, msg_name, mode)
                        );
                    }
                }
            }
            ~Publisher() {}

            virtual void publish(Msg message) = 0;

        protected:
            boost::shared_ptr<ITPS::MsgChannel<Msg>> channel;
    };

    template <typename Msg>
    class FieldPublisher : public Publisher<Msg> {
        public:

            FieldPublisher(std::string topic_name, std::string msg_name, Msg default_msg) 
                : Publisher<Msg>(topic_name, msg_name, "NB") {
                this->channel->set_msg(default_msg); // this avoids dealing with nullpointer exception 
                                               // if the msg type is not primitive when subscriber 
                                               // pull latest msg before publisher ever published anything
            }
            void publish(Msg message) {
                // boost::this_thread::sleep_for(boost::chrono::microseconds(AVOID_STARVATION_DELAY));
                this->channel->set_msg(message);
            }    

    };


    template <typename Msg>
    class MQPublisher : public Publisher<Msg> {
        public:
            MQPublisher(std::string topic_name, std::string msg_name) 
                : Publisher<Msg>(topic_name, msg_name, "B") {}

            void publish(Msg message) {
                this->channel->enqueue_msg(message);
            }

            void publish(Msg message, unsigned int timeout_ms) {
                this->channel->enqueue_msg(message, timeout_ms);
            }    
    };
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////





//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    template <typename Msg>
    class Subscriber {
        public:

            Subscriber(std::string topic_name, std::string msg_name, std::string mode) {
                this->topic_name = topic_name;
                this->msg_name = msg_name;
                this->mode = mode;
            }
            ~Subscriber() {}

            /* wait until a matching publisher is found
             * key = "topic_name.msg_name.mode".
             * the msg channel is created during the constructing phase
             * of the corresponding publisher with the same key string.
             * The MsgChannel object is stored in a internally global hash-map
             */
            virtual void subscribe() {
                do {
                    this->channel = MsgChannel<Msg>::get_channel(this->topic_name, this->msg_name, this->mode);
                } while(this->channel == nullptr);
            }

            /* return true if finding a msg channel with matching key string.
             * return false if timeout
             * key = "topic_name.msg_name.mode".
             * the msg channel is created during the constructing phase
             * of the corresponding publisher with the same key string.
             * The MsgChannel object is stored in a internally global hash-map
             */
            virtual void subscribe(unsigned int timeout_ms) {
                unsigned int t0 = millis();
                do {
                    if(millis() - t0 >= timeout_ms) {
                        throw std::runtime_error("Subscribe() TimeOut Exception, Cannot find a matching Publisher");
                    }
                    this->channel = MsgChannel<Msg>::get_channel(this->topic_name, this->msg_name, this->mode);
                } while(this->channel == nullptr);
            }

        protected:
            MsgChannel<Msg> *channel = nullptr;
            std::string topic_name, msg_name, mode;

        private:
            static unsigned int millis(void) {
                auto t = boost::chrono::high_resolution_clock::now();
                return (unsigned int)(double(t.time_since_epoch().count()) / 1000000.00f);
            }
    };



    template <typename Msg>
    class FieldSubscriber : public Subscriber<Msg> {
        public:
            FieldSubscriber(std::string topic_name, std::string msg_name) 
                : Subscriber<Msg>(topic_name, msg_name, "NB") {}


            void subscribe() {
                Subscriber<Msg>::subscribe();
            }


            void subscribe(unsigned int timeout_ms) {
                try { 
                    Subscriber<Msg>::subscribe(timeout_ms);
                }
                catch(std::exception& e) {
                    throw std::runtime_error(e.what());
                }
            }

            // Non-blocking Mode getter method
            Msg getMsg() {
                // non-blocking
                Msg rtn = this->channel->get_msg();
                // boost::this_thread::sleep_for(boost::chrono::microseconds(AVOID_STARVATION_DELAY));
                return rtn;
            }   

            // method reserved for special use case only
            void forceSetMsg(Msg msg) {
                this->channel->set_msg(msg);
            }

    };



    template <typename Msg>
    class MQSubscriber : public Subscriber<Msg> {
        public:
            //with message queue of size 1
            MQSubscriber(std::string topic_name, std::string msg_name) 
                : Subscriber<Msg>(topic_name, msg_name, "B") {
                msg_queue = boost::shared_ptr<ConsumerProducerQueue<Msg>>(
                    new ConsumerProducerQueue<Msg>(1) 
                );
            } 
    
            //with message queue of size queue_size
            MQSubscriber(std::string topic_name, std::string msg_name, unsigned int queue_size) 
                : Subscriber<Msg>(topic_name, msg_name, "B") {
                msg_queue = boost::shared_ptr<ConsumerProducerQueue<Msg>>(
                    new ConsumerProducerQueue<Msg>(queue_size) 
                );
            } 

            void subscribe() {
                Subscriber<Msg>::subscribe();
                this->channel->add_msg_queue(msg_queue);
            }    

            void subscribe(unsigned int timeout_ms) {
                try {
                    Subscriber<Msg>::subscribe(timeout_ms);
                }
                catch(std::exception& e){
                    throw std::runtime_error(e.what());   
                } 
                this->channel->add_msg_queue(msg_queue);
            }

            // For Message Queue Mode only
            Msg getMsg() {
                // conditionally blocking
                return msg_queue->consume();
            }

            // with time limit, if surpassing the timeout limit, return dft_rtn (default return value) 
            Msg getMsg(unsigned int timeout_ms, Msg dft_rtn) const {
                return msg_queue->consume(timeout_ms, dft_rtn);
            }

        protected:
            boost::shared_ptr<ConsumerProducerQueue<Msg>> msg_queue;
    };



    template <typename Msg>
    class FieldPubSubPair {
        public: 
            FieldPublisher<Msg>* pub;
            FieldSubscriber<Msg>* sub;

            FieldPubSubPair(std::string topic_name, std::string msg_name, Msg default_msg) {
                pub = new FieldPublisher<Msg>(topic_name, msg_name, default_msg);
                sub = new FieldSubscriber<Msg>(topic_name, msg_name);
                sub->subscribe();
            }
            ~FieldPubSubPair() {
                delete pub;
                delete sub;
            }
    };


    template <typename Msg>
    class MQPubSubPair {
        public: 
            MQPublisher<Msg>* pub;
            MQSubscriber<Msg>* sub;

            MQPubSubPair(std::string topic_name, std::string msg_name) {
                pub = new MQPublisher<Msg>(topic_name, msg_name);
                sub = new MQSubscriber<Msg>(topic_name, msg_name);
                sub->subscribe();
            }
            MQPubSubPair(std::string topic_name, std::string msg_name, unsigned int queue_size) {
                pub = new MQPublisher<Msg>(topic_name, msg_name);
                sub = new MQSubscriber<Msg>(topic_name, msg_name, queue_size);
                sub->subscribe();
            }
            ~MQPubSubPair() {
                delete pub;
                delete sub;
            }
    };

}
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////







// hash table storing messages with topic_name+msg_name as key
template <typename Msg>
std::unordered_map<std::string, ITPS::MsgChannel<Msg>*> ITPS::MsgChannel<Msg>::msg_table;

template <typename Msg>
boost::shared_mutex ITPS::MsgChannel<Msg>::table_mutex;
