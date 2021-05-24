library(argparse)
parser <- ArgumentParser()
parser$add_argument("--jobmonr-loc",
                    help = "The location of jobmonr")

args <- parser$parse_args()
jobmonr_loc <- args$jobmonr_loc

library(devtools)
# install from source an in-memory package
install_local(jobmonr_loc)