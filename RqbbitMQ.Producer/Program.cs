using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace RqbbitMQ.Producer
{
    public partial class Program
    {
        /* 
            A producer is a user application that sends messages.
            A queue is a buffer that stores messages.
            A consumer is a user application that receives messages.
         */

        static void Main(string[] args)
        {
            //Lesson1();
            //Lesson2();
            //Lesson3();
            //Lesson4();
            Lesson5();
        }
    }
}
