import React, { ReactNode, HTMLAttributes } from 'react';

interface SectionTitleProps extends HTMLAttributes<HTMLHeadingElement> {
  children: ReactNode;
}

const SectionTitle1: React.FC<SectionTitleProps> = (props) => {
  return (
    <h1
      {...props}
      className= {"text-2xl font-bold " + props.className}
    >
      {props.children}
    </h1>
  );
};

const SectionTitle2: React.FC<SectionTitleProps> = (props) => {
  return (
    <h2
      {...props}
      className= {"text-xl font-bold " + props.className}
    >
      {props.children}
    </h2>
  );
};

const SectionTitle3: React.FC<SectionTitleProps> = (props) => {
  return (
    <h3
      {...props}
      className= {"text-l font-bold " + props.className}
    >
      {props.children}
    </h3>
  );
};

const SectionTitle4: React.FC<SectionTitleProps> = (props) => {
  return (
    <h4
      {...props}
      className= {"text-m font-bold " + props.className}
    >
      {props.children}
    </h4>
  );
};



export { SectionTitle1, SectionTitle2, SectionTitle3, SectionTitle4 };