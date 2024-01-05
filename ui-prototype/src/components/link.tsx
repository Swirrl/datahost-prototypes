import React, { ReactNode, AnchorHTMLAttributes } from 'react';

interface LinkProps extends AnchorHTMLAttributes<HTMLAnchorElement> {
  children: ReactNode;
  external?: boolean; // Optional prop to indicate if the link is external
}

const Link: React.FC<LinkProps> = ({ children, external = false, ...props }) => {
  // If it's an external link, open in a new tab/window
  const target = external ? "_blank" : undefined;

  return (
    <a
      href={props.href}
      target={target}
      rel={external ? "noopener noreferrer" : undefined} // Add rel attribute for security
      className="text-sky-600 hover:text-sky-800 visited:text-indigo-800 active:text-gray-950
      hover:underline"
      {...props}
    >
      {children}
    </a>
  );
};

export default Link;