#!/bin/bash

#####################################################################

#System Specific
sys="cgwater"

######################################################################

#Directories
gpath="/sw/linux/gromacs/5.1.2/bin/"
suff=""

######################################################################

#Executables
gmx=${gpath}gmx
#grompp="$bin/grompp${suff2}"
#mdrun="$bin/mdrun${suff}"
#tpbconv="$bin/tpbconv${suff}"

######################################################################

#File Names
mdp="${sys}.mdp"
mdout="mdout-${sys}.mdp"
gro="../Equil_NPT/${sys}.confout.gro"
top="${sys}.top"
edr="${sys}.edr"
tpr="${sys}.tpr"
oldtrr="${sys}.PAR.trr"
trr="${sys}.trr"
gro_out="${sys}.confout.gro"
md_log="mdlog-${sys}.log"
mpirun="/home/theorie/rudzinski/soft/anaconda/envs/PyEmma-new/bin/mpirun"
gr_log="grompp.log"
md_log="mdrun.log"
cpi="state_old.cpt"
time="1000"
frame="4641"
table="table.xvg"

#####################################################################

#grompp
#Normal
$gmx grompp -f $mdp -c $gro -p $top -o $tpr -po $mdout -n index.ndx >& $gr_log

#Extend
#$grompp -f $mdp -c $oldtpr -p $top -o $newtpr -po $mdout\
#        >& $gr_log

#$tpbconv -s $newtpr -extend $time -o $new2tpr 

#use trr file for starting conf and velocities
#$grompp -f $mdp -c $gro -t $oldtrr -n index.ndx -time $frame -p $top -o $tpr -po $mdout\
#        >& $gr_log

#Tables
#$grompp -f $mdp -c $gro -p $top -o $tpr -po $mdout\
#        -n $ndx >& $gr_log

#Suffle
#$grompp -f $mdp -c $gro -p $top -o $tpr -po $mdout\
#        -shuffle  -deshuf $gr_des >& $gr_log

#####################################################################

#mdrun
#normal
#$mpirun
#nnodes=16
$gmx mdrun -s $tpr -o $trr -c $gro_out -e $edr -v -g $md_log
#>& $md_log

#extend
#$mpirun $mdrun -s $new2tpr -cpi $cpi -o $trr -c $gro_out -n $nnodes -e $edr -g $md_log >& $md_log





