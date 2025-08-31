// 1.
select 
    dname,
    to_char(nvl(avg(nvl(sal + comm, sal)), 0), '$99,990.00') as Salary
from dept left outer join emp on (emp.deptno = dept.deptno)
group by dname
order by Salary desc;

// 2.
select dname, ename, job, to_char(sal, '$99,990.00') as SALARY,
    to_char(dept_sal, '$99,990.00') as "AVERAGE_SA",
    to_char(dept_sal - sal, '$99,990.00') as DIFF
from 
    emp 
join 
    dept on (emp.deptno = dept.deptno) 
join
    (select deptno as id, avg(sal) as dept_sal
    from emp
    group by emp.deptno) avg on (avg.id = dept.deptno)
where sal < dept_sal
order by dept_sal - sal desc;

// 3.
select empno, ename, job, to_char(hiredate, 'YYYY') as HIRE
from emp
where empno in (
    select mgr
    from emp
    where mgr is not null
)
order by ename;

// 4.
select empno, ename, job, to_char(hiredate, 'YYYY') as HIRE,
rep.num_rep as "# Subalternos"
from emp
join (
    select mgr, count(*) as num_rep
    from emp
    where mgr is not null
    group by mgr
) rep on (emp.empno = rep.mgr)
order by rep.num_rep desc;

// 5.
select empno, ename, job, to_char(hiredate, 'DD-MON-YY') as HIREDATE,
    trunc((sysdate - hiredate) / 365) || ' años y ' || 
    trunc(mod(months_between(sysdate, hiredate), 12)) || ' meses' AS "Tiempo en la compañía"
from emp;

// 6.
select ename, job, to_char(hiredate, 'DD-MON-YY') as HIREDATE,
    trunc((sysdate - hiredate) / 365) || ' años y ' || 
    trunc(mod(months_between(sysdate, hiredate), 12)) || ' meses' AS "Contratación"
from emp
where empno in (
    select mgr
    from emp
    where mgr is not null
) and ( hiredate = (
    select min(hiredate) 
    from emp
    where empno in (
        select mgr
        from emp
        where mgr is not null
    )
)
or hiredate = (
    select max(hiredate) 
    from emp
    where empno in (
        select mgr
        from emp
        where mgr is not null
    )
));

// 7.
select ename, sal
from emp
where sal in (
    select sal
    from (
        select sal, rownum as rn
        from emp
        order by sal
    )
    where rn = &N // :N también funciona, pero me gusta más la interfaz que despliega &N
)
order by ename;

// 8.
select empno, lpad(ename, length(ename) + (level * 2) - 2, ' ') as ENAME,
    to_char(sal, '$999,999.99') as SAL,
    to_char(nvl(comm, 0), '$999,990.00') as COMM,
    nvl(to_char(mgr), ' ') as MGR
from emp
start with lower(ename) = 'king'
connect by prior empno = mgr;

// 9.
select ename,  to_char(sal, '$999,999.99') as SAL,
    round((sal / total) * 100, 2) as PCT
from emp natural join (
    select sum(sal) as total
    from emp
)
order by PCT desc;

// 10.
select
    job,
    nvl(( select sum(sal) from emp where deptno = 10 and job = e.job ), 0) as "Dept10",
    nvl(( select sum(sal) from emp where deptno = 20 and job = e.job ), 0) as "Dept20",
    sum(sal) as TOTAL
from emp e
where deptno in (10, 20)
group by job
order by job;
