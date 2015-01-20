#SSH into WRDS and type the following commands:
# mkdir ~/.ssh
# ssh-keygen -t rsa
#    NOTE: Enter password twice
# cat ~/.ssh/id_rsa.pub | ssh bdaughdr@wrds.wharton.upenn.edu "cat >> ~/.ssh/authorized_keys"
#    NOTE: Enter password


date <- as.Date("2012-01-06")
ticker <- "GM"

generate_sas_code <- function(ticker, date, tq="t") {
  # This function generates SAS code that will be piped into SAS
  paste("
   PROC SQL;
        CREATE TABLE temp AS
        SELECT * FROM crsp.Fund_hdr;
    quit;
        
        proc export data=temp
        outfile=stdout
        dbms=csv;
        run;", sep="")
}

get_data <- function(ticker, date, tq="t") {
  
  #ticker <- ticker
  #date <- date
  #tq <- "t"
  
  sas_code <- generate_sas_code(ticker, date, tq)
  temp_file <- tempfile()
  # This command calls SAS on the remote server.
  # -C means "compress output" ... this seems to have an impact even though we're
  # using gzip for compression of the CSV file spat out by SAS after it's
  # been transferred to the local computer (trial and error suggested this was
  # the most efficient approach).
  # -stdio means that SAS will take input from STDIN and output to STDOUT
  sas_command <- paste("ssh -C bdaughdr@wrds.wharton.upenn.edu ",
                       "'sas -stdio -noterminal' | gzip > ",
                       temp_file)
  
  # The following pipes the SAS code to the SAS command. The "intern=TRUE"
  # means that we can capture the output in an R variable.
  system_call <- paste("echo '", sas_code, "' |", sas_command)
  system(system_call, intern=TRUE)
  read.csv(temp_file, as.is=TRUE)
}

# Now get the data from WRDS
system.time(trades <- get_data(ticker, date, tq="t"))
system.time(quotes <- get_data(ticker, date, tq="q"))