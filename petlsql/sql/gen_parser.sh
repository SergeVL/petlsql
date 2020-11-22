
docker run -t --rm -u 1000 -v `pwd`:/build -w /build registry.master.cns/cocor:latest sql.atg
if [ -f Scanner.py.old ]; then
    rm *.old
fi