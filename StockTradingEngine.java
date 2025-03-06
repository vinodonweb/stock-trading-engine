import java.util.*;
import java.util.concurrent.*;

public class StockTradingEngine {

    //maximum number of stocks
    private static final int MAX_STOCKS = 1024;
    //lock free order queue : one array for buy orders and one for sell orders
    @SuppressWarnings("unchecked")
    private final ConcurrentLinkedDeque<OrderProcessor>[] buyOrders = new ConcurrentLinkedDeque[MAX_STOCKS];

    @SuppressWarnings("unchecked")
    private final ConcurrentLinkedDeque<OrderProcessor>[] sellOrders = new ConcurrentLinkedDeque[MAX_STOCKS];

    //constructor to initialize the buy and sell order queues
    public StockTradingEngine() {
        for (int i = 0; i < MAX_STOCKS; i++) {
            buyOrders[i] = new ConcurrentLinkedDeque<>();
            sellOrders[i] = new ConcurrentLinkedDeque<>();
        }
    }

    //order processing method
    private static class OrderProcessor {
        String orderType;
        String ticker;
        int quantity;
        double price;
        long timestamp; //time of order

        public OrderProcessor(String orderType, String ticker, int quantity, double price) {
            this.orderType = orderType;
            this.ticker = ticker;
            this.quantity = quantity;
            this.price = price;
            this.timestamp = System.nanoTime();
        }
    }

    //Map a ticker symbol to a unique integer
    private int getStockIndex(String ticker) {
        return Math.abs(ticker.hashCode()) % MAX_STOCKS;
    }

    /**
     * Add an order to the order book
     * @param orderType "Buy" or "Sell"
     * @param ticker stock identifier
     * @param quantity number of shares
     * @param price Price per share
     */

    public void addOrder(String orderType, String ticker, int quantity, double price){
        int index = getStockIndex(ticker);
        OrderProcessor order = new OrderProcessor(orderType, ticker, quantity, price);
        if(orderType.equalsIgnoreCase("Buy")) {
            buyOrders[index].offer(order);
        } else if(orderType.equalsIgnoreCase("Sell")) {
            sellOrders[index].offer(order);
        }
    }

    /**
     * Matches a buy order with a sell order for the given ticker
     * matching criteria: Buy price >= lowest sell price available
     *
     * @param ticker The stock ticker for which to match orders.
     */

    public void matchOrder(String ticker){
        int index = getStockIndex(ticker);
        ConcurrentLinkedDeque<OrderProcessor> buys = buyOrders[index];
        ConcurrentLinkedDeque<OrderProcessor> sells = sellOrders[index];

        //find the sell order with the lowest price
        OrderProcessor lowestSell = null;
        for(OrderProcessor orderProcessor : sells){
            if(lowestSell == null || orderProcessor.price < lowestSell.price){
                lowestSell = orderProcessor;
            }
        }

        if(lowestSell == null){
            //no sell order to match
            return;
        }

        //lock for a buy order that can cover the lowest sell
        for(OrderProcessor buyOrder : buys){
           if(buyOrder.price >= lowestSell.price){
               //execute transaction: remove both orders from the queue
                if(sells.remove(lowestSell) && buys.remove(buyOrder)){
                    System.out.println("Matched " + ticker + ": Buy order at $" + buyOrder.price +
                            " with Sell order at $" + lowestSell.price +
                            " for " + Math.min(buyOrder.quantity, lowestSell.quantity) + " shares");
                }

                break;
           }
        }
    }

    /**
     * iterates through all stocks to attempt matching orders.
     */

    public void matchAllOrders(){
        for(int i = 0; i < MAX_STOCKS; i++){
            //if both buy and sell queues have orders, attempt to match
            if(!buyOrders[i].isEmpty() && !sellOrders[i].isEmpty()){
                //use any order's ticker from the sell queue
                OrderProcessor sample = sellOrders[i].peek();
                if(sample != null){
                    matchOrder(sample.ticker);
                }
            }
        }
    }

    public static void main(String[] args) {
        StockTradingEngine stockTradingEngine = new StockTradingEngine();
       String[] tickers = {"AAPL", "GOOG", "MSFT", "AMZN", "FB"};
        Random random = new Random();

        //runnable to generate random orders.

        Runnable orderGenerator = () -> {
            for (int i = 0; i < 100; i++){
                String ticker = tickers[random.nextInt(tickers.length)];
                String orderType = random.nextBoolean() ? "Buy" : "Sell";
                int quantity = random.nextInt(100) + 1; //1 to 100 share
                double price = 100 + random.nextDouble() * 50; //price between 100 and 150
                stockTradingEngine.addOrder(orderType, ticker, quantity, price);

                try {
                    Thread.sleep(random.nextInt(50)); //short pause
                } catch (InterruptedException e){
                    Thread.currentThread().isInterrupted();
                }
            }
        };

        //start the multiple thread
        int numOfThread = 5;
        Thread[] threads = new Thread[numOfThread];
        for(int i = 0; i < numOfThread; i++){
            threads[i] = new Thread(orderGenerator);
            threads[i].start();
        }

        //wait for all order generator threads to complete
        for(int i = 0; i < numOfThread; i++){
            try {
                threads[i].join();
            } catch (InterruptedException e){
                Thread.currentThread().interrupt();
            }
        }

        //end task after orders are added.
        stockTradingEngine.matchAllOrders();
    }
}