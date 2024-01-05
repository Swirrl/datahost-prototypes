import React, { ReactNode, HTMLAttributes } from 'react';
import { SectionTitle1, SectionTitle4 } from './sectionTitle';

interface HeaderSectionProps extends HTMLAttributes<HTMLDivElement> {
  title: string;
  description: string;
  dateCreated?: string;
  dateModified?: string;
}

const HeaderSection: React.FC<HeaderSectionProps> = ({ title, description, dateCreated, dateModified, ...props }) => {
  const formatHumanReadableDate = (dateString: string): string => {
    const options: Intl.DateTimeFormatOptions  = { year: 'numeric', month: 'long', day: 'numeric' };
    return new Date(dateString).toLocaleDateString(undefined, options);
  };

  return (
    <div {...props}>
      <SectionTitle1>{title}</SectionTitle1>
      {dateCreated && dateModified && dateCreated === dateModified && (
        <SectionTitle4>
            Created: {formatHumanReadableDate(dateCreated)} <span className='font-light'>({dateCreated})</span>
          </SectionTitle4>
      )}
      {dateCreated && dateModified && dateCreated!== dateModified &&(
        <>
        <SectionTitle4>
            Created: {formatHumanReadableDate(dateCreated)} <span className='font-light'>({dateCreated})</span>
          </SectionTitle4>
        <SectionTitle4>
          Last Modified: {formatHumanReadableDate(dateModified)} <span className='font-light'>({dateModified})</span>
        </SectionTitle4>
          </>
      )}
      {dateCreated && !dateModified && (
        <SectionTitle4>
          Created on: {formatHumanReadableDate(dateCreated)} <span className='font-light'>({dateCreated})</span>
        </SectionTitle4>
      )}
      {!dateCreated && dateModified && (
        <SectionTitle4>
          Last Modified: {formatHumanReadableDate(dateModified)} <span className='font-light'>({dateModified})</span>
        </SectionTitle4>
      )}

      <p className="text-gray-600 py-2">{description}</p>
    </div>
  );
};

export default HeaderSection;